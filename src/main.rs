use clap::{arg, command};
use csv::{Reader, ReaderBuilder, Trim};
use env_logger::{Builder, Env};
use log::{debug, error, trace, warn};
use rust_decimal::Decimal;
use serde::{de, Deserialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::path::Path;
use std::str::FromStr;

fn validate_input(input: Option<&OsStr>) -> io::Result<&Path> {
    let err_str = "Invalid! Input must be path to file that exists on the filesystem.";
    if let Some(transactions_csv) = input {
        let possible_path = Path::new(transactions_csv);
        if possible_path.exists() {
            Ok(possible_path)
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, err_str))
        }
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidInput, err_str))
    }
}

const PRECISION: u32 = 4u32;
//TODO you've hardcoded a value, if you had more time, you'd make this configurable via clap
pub fn deserialize_with_precision_of_4<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: de::Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    from_string_with_precision(&buf, PRECISION).map_err(de::Error::custom)
}

fn from_string_with_precision(val: &str, precision: u32) -> Result<Decimal, rust_decimal::Error> {
    if val.is_empty() {
        Ok(Decimal::ZERO)
    } else {
        Decimal::from_str(val).map(|decimal| decimal.round_dp(precision))
    }
}

#[derive(Debug, Copy, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Withdrawal,
    Deposit,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Deserialize, Debug, Copy, Clone)]
struct Record {
    #[serde(rename = "type")]
    transaction_type: TransactionType,
    #[serde(rename = "client")]
    client_id: u16,
    #[serde(rename = "tx")]
    transaction_id: u32,
    #[serde(deserialize_with = "deserialize_with_precision_of_4")]
    amount: Decimal,
}

/// as in, a record that has some context. In this case, embedding a "chronological" element.
/// The app is currently not "stateful" a full implementation would track monotonic_counter offsets
/// in some crash-safe persistent store to guarantee monotonicty.
#[derive(Debug, Copy, Clone)]
struct SituatedRecord {
    monotonic_counter: usize,
    record: Record,
}

#[derive(Debug)]
struct ClientState {
    client_id: u16,
    available_funds: Decimal,
    held_funds: Decimal,
    locked: bool,
    // TODO Vec<SituatedRecord> by convention stores records with the same transaction_id like
    // [(Withdrawal|Deposit),(Dispute),(Resolution|Chargeback)] in a Vec in that order,
    // this convention would be better understood with an API
    client_transactions: HashMap<u32, Vec<SituatedRecord>>,
}

impl ClientState {
    fn new(client_id: u16) -> Self {
        ClientState {
            client_id,
            available_funds: Decimal::default(),
            held_funds: Decimal::default(),
            locked: false,
            client_transactions: HashMap::new(),
        }
    }

    fn get_available_funds(&self) -> Decimal {
        self.available_funds
    }

    fn get_held_funds(&self) -> Decimal {
        self.held_funds
    }

    fn get_total_funds(&self) -> Decimal {
        self.held_funds + self.available_funds
    }

    fn is_locked(&self) -> bool {
        self.locked
    }

    /// return the last processed counter, in a persistent system, this means said record is now durable.
    /// a crash safe persistent system should indicate the last record it actually processed
    /// so restarts are possible.
    fn add_transaction(&mut self, situated_record: SituatedRecord) -> usize {
        let tx_id = situated_record.record.transaction_id;
        let transact = self.transact(situated_record);
        if transact {
            self.push_transaction(tx_id, situated_record);
        }
        situated_record.monotonic_counter
    }

    fn transact_withdrawal_or_deposit(&mut self, situated_record: SituatedRecord) -> bool {
        let amount = situated_record.record.amount;
        let tx_type = situated_record.record.transaction_type;
        let tx_id = situated_record.record.transaction_id;
        match (tx_type, self.locked) {
            (TransactionType::Withdrawal, false) => {
                if amount <= self.available_funds {
                    self.available_funds -= amount;
                } else {
                    warn!(
                        "Withdrawal ({}) failed to withdraw due to insufficient funds.",
                        tx_id
                    );
                }
                true
            }
            (TransactionType::Withdrawal, true) => {
                warn!(
                    "Withdrawal ({}) failed to process because client account ({}) is frozen.",
                    tx_id, self.client_id
                );
                true
            }
            (TransactionType::Deposit, _) => {
                self.available_funds += amount;
                true
            }
            (_, _) => false,
        }
    }

    fn push_transaction(&mut self, tx_id: u32, record: SituatedRecord) {
        self.client_transactions
            .entry(tx_id)
            .or_insert_with(Vec::new)
            .push(record);
    }

    fn transact(&mut self, situated_record: SituatedRecord) -> bool {
        let tx_id = situated_record.record.transaction_id;
        let client_id = situated_record.record.client_id;
        let len = if let Some(vec) = self
            .client_transactions
            .get(&situated_record.record.transaction_id)
        {
            vec.len()
        } else {
            0
        };
        trace!(
            "Type {:?}, id {}, len of transactions vec is {}.",
            situated_record.record.transaction_type,
            tx_id,
            len
        );
        match (situated_record.record.transaction_type, self.locked) {
            (TransactionType::Withdrawal | TransactionType::Deposit, _) => {
                // must have original withdrawal/deposit transaction ids
                if len == 0 {
                    self.transact_withdrawal_or_deposit(situated_record)
                } else {
                    warn!("Record of type ({:?}) is re-using existent transaction id ({}), this is not allowed!)", situated_record.record.transaction_type, tx_id);
                    false
                }
            }
            //TOD0 self.locked needs to behave differently for disputes/resolves/chargebacks
            (TransactionType::Dispute, false) => {
                if len == 1 {
                    self.transact_dispute(situated_record)
                } else {
                    warn!("Dispute [transaction_id={}, client_id={}] will be ignored as it either does not exist or has already been addressed.", tx_id, client_id);
                    false
                }
            }
            (TransactionType::Resolve | TransactionType::Chargeback, false) => {
                if len == 2 {
                    self.transaction_resolution(situated_record)
                } else {
                    warn!("Resolution/Chargeback for transaction ({}) will be ignored as it has already been addressed.", tx_id);
                    false
                }
            }
            (
                TransactionType::Resolve | TransactionType::Chargeback | TransactionType::Dispute,
                true,
            ) => {
                warn!(
                    "Resolution/Chargeback/Dispute  ({}) failed to process because client account ({}) is frozen.",
                    tx_id, client_id);
                false
            }
        }
    }

    fn transact_dispute(&mut self, dispute: SituatedRecord) -> bool {
        let tx_id = dispute.record.transaction_id;
        if let Some(all_prev_record) = self.client_transactions.get(&tx_id) {
            let disputed_target = all_prev_record.iter().find(|record| {
                matches!(record.record.transaction_type, TransactionType::Withdrawal)
                    || matches!(record.record.transaction_type, TransactionType::Deposit)
            });
            if let Some(disputed_target) = disputed_target {
                match disputed_target.record.transaction_type {
                    TransactionType::Withdrawal => {
                        let prev_amount = disputed_target.record.amount;
                        self.held_funds += prev_amount;
                    }
                    TransactionType::Deposit => {
                        let prev_amount = disputed_target.record.amount;
                        self.available_funds -= prev_amount;
                        self.held_funds += prev_amount;
                    }
                    _ => {}
                }
                true
            } else {
                warn!("Dispute for transaction id ({:?}) will be ignored as it does not refer to an extant withdrawal or deposit.", tx_id);
                false
            }
        } else {
            error!("Internal state of records for transaction id ({}) is incorrect, offending transaction history: {:?}.", tx_id, self.client_transactions.get(&tx_id));
            false
        }
    }

    fn transaction_resolution(&mut self, resolution: SituatedRecord) -> bool {
        let tx_id = resolution.record.transaction_id;
        if let Some(all_prev_record) = self.client_transactions.get(&tx_id) {
            let prev_record = all_prev_record.iter().find(|record| {
                matches!(record.record.transaction_type, TransactionType::Withdrawal)
                    || matches!(record.record.transaction_type, TransactionType::Deposit)
            });
            let transact = if let Some(prev_record) = prev_record {
                (
                    Some(prev_record.record.transaction_type),
                    Some(prev_record.record.amount),
                )
            } else {
                error!("Resolve for transaction id ({}) will be ignored as it does not refer to an existing withdrawal or deposit.", tx_id);
                (None, None)
            };
            match transact {
                (Some(tx_type), Some(tx_amount)) => match resolution.record.transaction_type {
                    TransactionType::Resolve => self.transact_resolve(tx_type, tx_amount),
                    TransactionType::Chargeback => self.transact_chargeback(tx_type, tx_amount),
                    _ => false,
                },
                (_, _) => false,
            }
        } else {
            error!("Internal state of records for transaction id ({}) is incorrect, offending transaction history: {:?}.", tx_id, self.client_transactions.get(&tx_id));
            false
        }
    }
    fn transact_resolve(&mut self, prev_type: TransactionType, tx_amount: Decimal) -> bool {
        match prev_type {
            TransactionType::Withdrawal | TransactionType::Deposit => {
                self.held_funds -= tx_amount;
                self.available_funds += tx_amount;
                true
            }
            _ => false,
        }
    }

    fn transact_chargeback(&mut self, prev_type: TransactionType, tx_amount: Decimal) -> bool {
        match prev_type {
            TransactionType::Withdrawal | TransactionType::Deposit => {
                self.held_funds -= tx_amount;
                self.locked = true;
                true
            }
            _ => false,
        }
    }
}

fn process_record(situated_record: SituatedRecord, clients: &mut HashMap<u16, ClientState>) {
    let client_id = situated_record.record.client_id;
    if let Some(client_state) = clients.get_mut(&client_id) {
        client_state.add_transaction(situated_record);
    } else {
        let mut client_state = ClientState::new(client_id);
        client_state.add_transaction(situated_record);
        clients.insert(client_id, client_state);
    }
}

fn get_reader(path: &Path) -> Result<Reader<File>, csv::Error> {
    let reader = ReaderBuilder::new().trim(Trim::All).from_path(path);
    reader
}

fn play_with_money(
    input: Option<&OsStr>,
    clients: &mut HashMap<u16, ClientState>,
) -> io::Result<()> {
    let records_input = validate_input(input)?;
    let reader = get_reader(records_input)?;
    for (monotonic_counter, record) in reader.into_deserialize().enumerate() {
        let record = record?;
        let situated_record = SituatedRecord {
            monotonic_counter,
            record,
        };
        process_record(situated_record, clients);
    }
    Ok(())
}

fn main() {
    Builder::from_env(Env::default().default_filter_or("off")).init();

    let matches = command!()
        .arg(
            arg!([transactions_csv])
                .help("CSV file containing chronological list of client transactions"),
        )
        .get_matches();
    let str = matches.value_of("transactions_csv").map(|s| s.as_ref());

    debug!("Given filepath: {:?}.", &str);
    let mut clients = HashMap::new();
    match play_with_money(str, &mut clients) {
        Ok(_) => match write_client_state(&clients) {
            Ok(_) => {
                debug!("done processing!");
            }
            Err(e) => {
                error!("Encountered error while processing data!\n{}", e);
            }
        },
        Err(e) => {
            error!("Encountered error while processing data!\n{}", e);
        }
    }
}

fn write_client_state(clients: &HashMap<u16, ClientState>) -> Result<(), csv::Error> {
    let mut wtr = csv::Writer::from_writer(io::stdout());
    wtr.write_record(&["client", "available", "held", "total", "locked"])?;
    for x in clients.keys() {
        let client = clients.get(x);
        if let Some(client) = client {
            wtr.write_record(&[
                format!("{}", client.client_id),
                format!("{}", client.get_available_funds()),
                format!("{}", client.get_held_funds()),
                format!("{}", client.get_total_funds()),
                format!("{}", client.is_locked()),
            ])?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;

    /// Return the repo root directory path.
    fn repo_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    /// Return the directory containing the example data.
    fn data_dir() -> PathBuf {
        repo_dir().join("examples").join("data")
    }

    fn read_into_memory(reader: Reader<File>) -> io::Result<Vec<SituatedRecord>> {
        let mut all_records = vec![];
        for (monotonic_counter, record) in reader.into_deserialize().enumerate() {
            let record = record?;
            all_records.push(SituatedRecord {
                monotonic_counter,
                record,
            });
        }
        Ok(all_records)
    }

    fn read_records_into_memory(path: &Path) -> io::Result<Vec<SituatedRecord>> {
        let reader = get_reader(path)?;
        read_into_memory(reader)
    }

    #[test]
    fn test_reader() {
        let p = data_dir().join("sample.csv");
        let valid_input = validate_input(Some(p.as_os_str()));
        assert!(valid_input.is_ok());
        assert!(valid_input.unwrap().exists());
        let invalid_input1 = validate_input(None);
        assert!(invalid_input1.is_err());
        let invalid_input2 = validate_input(Some("this is nota filepath at all!".as_ref()));
        assert!(invalid_input2.is_err());
    }

    #[test]
    fn test_read_in_records_whitespace() {
        let p = data_dir().join("whitespace-sample.csv");
        let vec = read_records_into_memory(&*p).unwrap();
        assert_eq!(5, vec.len());
        let mut test_amounts: Decimal = Decimal::ZERO;
        for x in vec {
            test_amounts += x.record.amount;
        }
        assert_eq!(Decimal::new(96214, 4), test_amounts);
    }

    #[test]
    fn test_sample_csv() {
        let p = data_dir().join("sample.csv");
        let mut clients = HashMap::new();
        play_with_money(Some(p.as_os_str()), &mut clients).unwrap();
        for client_id in clients.keys() {
            let state = clients.get(client_id).unwrap();
            match client_id {
                1 => {
                    assert_eq!(Decimal::new(14848, 4), state.available_funds);
                    assert_eq!(Decimal::ZERO, state.held_funds);
                    assert!(!state.locked);
                }
                2 => {
                    assert_eq!(Decimal::new(80290, 4), state.available_funds);
                    assert_eq!(Decimal::ZERO, state.held_funds);
                    assert!(!state.locked);
                }
                3 => {
                    assert_eq!(Decimal::new(1000, 1), state.available_funds);
                    assert_eq!(Decimal::ZERO, state.held_funds);
                    assert!(state.locked);
                }
                4 => {
                    assert_eq!(Decimal::ZERO, state.available_funds);
                    assert_eq!(Decimal::new(-100, 0), state.held_funds);
                    assert!(!state.locked);
                }
                5 => {
                    assert_eq!(Decimal::new(10000, 2), state.available_funds);
                    assert_eq!(Decimal::ZERO, state.held_funds);
                    assert!(!state.locked);
                }
                _ => unreachable!(),
            }
        }
    }
}

// https://rust-lang-nursery.github.io/rust-cookbook/encoding/csv.html
// https://docs.rs/csv/1.1.6/csv/struct.Reader.html#method.deserialize
// https://crates.io/crates/serde
// https://docs.rs/serial_int/latest/serial_int/
