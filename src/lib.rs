#[cfg(feature = "finacials")]
pub mod balance_sheets;
#[cfg(feature = "finacials")]
pub mod cash_flows;
pub mod error;
#[cfg(feature = "finacials")]
pub mod income_statements;

pub const USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0";
pub const YAHOO_ROOT: &str = "https://finance.yahoo.com";
pub const INVESTING_ROOT: &str = "https://www.investing.com";

pub trait Spider {
    fn fetch(symbol: &str) -> Result<String, reqwest::Error>;
}
