# PLAYING WITH MONEY

This took me about 5 hours keyboard time. 

## on time
### Sticking points on"keyboard time"
1. It took me a bit to find the API for serde as this is my first time using the library.
	- I had to figure out the visitor api/annotations so I could read in Decimal values
	to let the Decimal library completely handle all concerns about precision. I knew
	such a thing should exist in a library like serde, but
	my first try at the api had me using the visitor pattern and I overrode methods like
	visit_f64 but then I realized based on my input I had to handle visit_i64 and visit_str 
	which not only become intractable but wasn't what I wanted! why? because serde 
	was reading in 100.00 as an f64 via visit_f64 as the literal 100f64 which was converted to a decimal
	with 0 precision instead of 2. I didn't discover this until I was testing some edge
	cases in sample.csv and so it took me more time to figure out that deserialize_with
	could be forced to read in strings only and the string values preserved the
	precision and worked properly with the Decimal::from_str api to preserve precision.
2. I wrestled with the borrow checker a bit while dealing with the vector of records
I was storing in a HashMap
	- The borrow checker is as the borrow checker does... sometimes it holds us up.
3. Because I like to experiment with my code in a pseudo-TDD environment I spent
time making a version I could test with that read things into memory, but adapted
a version for use in main that streams records, this refactoring took some time,
and with more foresight could have been avoided.

## on assumptions
### on account freezes
- Referenced investopedia and decided that after a chargeback, a frozen account
could accept transactions of type deposits and nothing else. 
- There is no way to unfreeze an account

### on disputes for withdrawals
- a disputed withdrawal will not decrement from available funds because a processed
withdrawal has already done so. Disputed withdrawals will only increment held funds
by amount and on Resolve will subtract amount from held funds and add to available_funds
otherwise a dispute for a withdrawal has no effect on a client account.

### on duplicate transactions types
- the only allowable state transitions are (withdraw/deposit)->dispute->(chargeback/resolve)
any transaction for the same transaction_id will be ignored.

### on unique transaction ids
- program implementation doesn't require them to be unique. in a persistent implementation
backed  by a key/value store this assumption would probably be relied upon so
transaction id could be used as key's regardless of client_id (current implemtnation
used in memory hash map)

### on precision tests
- I know that
```rust
                    assert_eq!(Decimal::new(10000, 2), Decimal::new(1000, 1));
```
doesn't panic which means my test cases do not properly confirm something I've confirmed
with my eyeballs and the irks me.


## comments
- Code is not persistent, but a "complete" implementation might be required to recognize if the same
data was read in multiple times, be restartable/recoverable without redoing work
or compromising client state. It's possible that:
1. no data is ever submitted multiple times,
2. all transactions can be re-streamed through the system if a crash/restart occurs.
However those are not the kind of assumptions I've found one can make in the real world.
- there is a hardcoded value, but it exists in one place.
- `cargo clippy` and `cargo fmt` were run using Rust 1.59.0
- *always* have a configurable logger
