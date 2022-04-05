use clap::{arg, command};
use csv::Reader;
use log::{debug, error, warn};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::{de, Deserialize};
use simple_logger::SimpleLogger;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;
use std::{fmt, io};

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

//TODO you've hardcoded a value, if you had more time, you'd make this configurable via clap
pub fn deserialize_with_precision_of_4<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: de::Deserializer<'de>,
{
    struct DecimalStringVisitor;

    impl<'de> de::Visitor<'de> for DecimalStringVisitor {
        type Value = Decimal;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("A string that can be converted to a decimal number")
        }

        fn visit_f64<E>(self, val: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            from_float_with_precision(val, 4).map_err(E::custom)
        }
    }

    deserializer.deserialize_any(DecimalStringVisitor)
}

fn from_float_with_precision(val: f64, precision: u32) -> Result<Decimal, rust_decimal::Error> {
    Ok(Decimal::from_f64(val)
        .ok_or(rust_decimal::Error::ErrorString(String::from(
            "Invalid float!",
        )))?
        .round_dp(precision))
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
struct SituatatedRecord {
    monotonic_counter: usize,
    record: Record,
}

#[derive(Debug)]
struct ClientState {
    client_id: u16,
    available_funds: Decimal,
    held_funds: Decimal,
    locked: bool,
    client_transactions: HashMap<u32, SituatatedRecord>,
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
    /// a crash safe persistent system should indicate the last record it actualyl processed
    /// so restarts are possible.
    fn add_transaction(&mut self, situated_record: SituatatedRecord) -> usize {
        let tx_id = situated_record.record.transaction_id;
        let tx_type = situated_record.record.transaction_type;
        match tx_type {
            TransactionType::Withdrawal | TransactionType::Deposit => {
                let amount = situated_record.record.amount;
                match (tx_type, self.locked) {
                    (TransactionType::Withdrawal, false) => {
                        if amount <= self.available_funds {
                            self.available_funds = self.available_funds - amount;
                        } else {
                            warn!("Withdrawal ({}) failed to process due to insufficient funds.", tx_id);
                        }
                    },
                    (TransactionType::Withdrawal, true) => {
                        warn!("Withdrawal ({}) failed to process because client account ({}) is frozen.", tx_id, self.client_id);
                    }
                    (TransactionType::Deposit, _) => {
                        self.available_funds = self.available_funds + amount;
                    },
                    (_, _) => unreachable!(),
                }
                self.client_transactions.insert(tx_id, situated_record);
            },
            TransactionType::Dispute => {},
            TransactionType::Resolve | TransactionType::Chargeback => {},
        }
        situated_record.monotonic_counter
    }
}

fn read_into_memory(reader: Reader<File>) -> io::Result<Vec<SituatatedRecord>> {
    let mut all_records = vec![];
    let mut monotonic_counter: usize = 0;
    for res in reader.into_deserialize() {
        let record: Record = res?;
        all_records.push(SituatatedRecord {
            monotonic_counter,
            record,
        });
        monotonic_counter = monotonic_counter + 1;
    }
    Ok(all_records)
}

fn read_records_into_memory(path: &Path) -> io::Result<Vec<SituatatedRecord>> {
    let reader = csv::Reader::from_path(path);
    let reader = reader?;
    read_into_memory(reader)
}

fn process_record(situated_record: SituatatedRecord, clients: &mut HashMap<u16, ClientState>) {
    let client_id = situated_record.record.client_id;
    if let Some(client_state) = clients.get_mut(&client_id) {
        client_state.add_transaction(situated_record);
    } else {
        let mut client_state = ClientState::new(client_id);
        client_state.add_transaction(situated_record);
        clients.insert(client_id, client_state);
    }
}

fn play_with_money(
    input: Option<&OsStr>,
    clients: &mut HashMap<u16, ClientState>,
) -> io::Result<()> {
    let records_input = validate_input(input)?;
    let reader = csv::Reader::from_path(records_input);
    let reader = reader?;
    let mut monotonic_counter: usize = 0;

    for record in reader.into_deserialize() {
        let record = record?;
        let situated_record = SituatatedRecord {
            monotonic_counter,
            record,
        };
        process_record(situated_record, clients);
        monotonic_counter = monotonic_counter + 1;
    }
    Ok(())
}

fn main() {
    SimpleLogger::new().env().init().unwrap();
    let matches = command!()
        .arg(
            arg!([transactions_csv])
                .help("CSV file containing chronological list of client transactions"),
        )
        .get_matches();
    let str = matches.value_of("transactions_csv").map(|s| s.as_ref());

    let mut clients = HashMap::new();
    match play_with_money(str, &mut clients) {
        Ok(_) => {
            for x in clients.keys() {
                let client = clients.get(x);
                println!("{:?}", client);
            }
            debug!("done processing!");
        }
        Err(e) => {
            error!("Encountered error while processing data!\n{}", e);
        }
    }
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
    fn test_records() {
        let p = data_dir().join("sample.csv");
        let vec = read_records_into_memory(&*p).unwrap();
        assert_eq!(5, vec.len());
        let mut test_amounts: Decimal = Decimal::new(0, 0);
        for x in vec {
            test_amounts += x.record.amount.round_dp(4);
        }
        assert_eq!(Decimal::new(96214, 4), test_amounts);
    }

    #[test]
    fn test_main() {
        let p = data_dir().join("sample.csv");
        let mut clients = HashMap::new();
        play_with_money(Some(p.as_os_str()), &mut clients).unwrap();
        for client_id in clients.keys() {
            let state = clients.get(client_id).unwrap();
            match client_id {
                1 => {
                    assert_eq!(Decimal::new(14848, 4), state.available_funds)
                }
                2 => {
                    assert_eq!(Decimal::new(20222, 4), state.available_funds)
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
