use crate::{Spider, USER_AGENT, YAHOO_ROOT};
use base64::prelude::*;
use chrono::{Datelike, NaiveDate};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};

#[cfg(feature = "postgres")]
pub mod database;

pub const BALANCE_SHEETS_SCHEMA_VERSION: i16 = 0;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct BalanceSheet {
    pub symbol: String,
    pub term: NaiveDate,
    pub total_assets: Option<String>,
    pub total_liabilities_net_minority_interest: Option<String>,
    pub total_equity_gross_minority_interest: Option<String>,
    pub total_capitalization: Option<String>,
    pub preferred_stock_equity: Option<String>,
    pub common_stock_equity: Option<String>,
    pub net_tangible_assets: Option<String>,
    pub invested_capital: Option<String>,
    pub tangible_book_value: Option<String>,
    pub total_debt: Option<String>,
    pub net_debt: Option<String>,
    pub share_issued: Option<String>,
    pub ordinary_shares_number: Option<String>,
    pub preferred_shares_number: Option<String>,
    pub treasury_shares_number: Option<String>,
    pub working_capital: Option<String>,
    pub capital_lease_obligations: Option<String>,
    #[cfg(feature = "postgres")]
    pub filed: NaiveDate,
    pub version: i16,
}

impl BalanceSheet {
    pub fn parse(html: &str, symbol: &str) -> Vec<BalanceSheet> {
        let document = Html::parse_document(html);
        let rows = Selector::parse(".tableBody .row").unwrap();
        let mut year1 = vec![];
        let mut year2 = vec![];
        let mut year3 = vec![];
        let mut year4 = vec![];
        let mut year5 = vec![];
        let mut titles = vec![];
        let mut terms = BalanceSheet::get_terms(&document).into_iter();
        let mut res = vec![];

        for row in document.select(&rows) {
            let columns_html = row.inner_html();
            let fragment = Html::parse_fragment(&columns_html);
            let columns = Selector::parse(".column").unwrap();

            for (column_count, column) in fragment.select(&columns).enumerate() {
                if column_count == 0 {
                    let column_html = column.html();
                    let fragment = Html::parse_fragment(&column_html);
                    let column_div_selector = Selector::parse(".rowTitle").unwrap();
                    let content = fragment.select(&column_div_selector).next().unwrap();
                    let t = content
                        .text()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .trim()
                        .to_string();

                    titles.push(t);
                } else {
                    let column_html = column.html();
                    let fragment = Html::parse_fragment(&column_html);
                    let column_div_selector = Selector::parse("div").unwrap();
                    let content = fragment.select(&column_div_selector).next().unwrap();
                    let t = content
                        .text()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .trim()
                        .to_string();

                    if column_count == 1 {
                        year1.push(t);
                    } else if column_count == 2 {
                        year2.push(t);
                    } else if column_count == 3 {
                        year3.push(t);
                    } else if column_count == 4 {
                        year4.push(t);
                    } else if column_count == 5 {
                        year5.push(t);
                    }
                }
            }
        }

        if let Some(term) = terms.next() {
            res.push(BalanceSheet::from_vec(&titles, year1, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(BalanceSheet::from_vec(&titles, year2, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(BalanceSheet::from_vec(&titles, year3, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(BalanceSheet::from_vec(&titles, year4, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(BalanceSheet::from_vec(&titles, year5, &term, symbol));
        };

        res
    }

    fn get_terms(html: &Html) -> Vec<String> {
        let headers = Selector::parse(".tableHeader .column").unwrap();
        let mut titles = vec![];

        for (header_count, header) in html.select(&headers).enumerate() {
            if header_count != 0 {
                let text = header
                    .text()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .trim()
                    .to_string();

                titles.push(text);
            }
        }

        titles
    }

    fn from_vec(titles: &[String], values: Vec<String>, term: &str, symbol: &str) -> Self {
        let mut balance_sheet = BalanceSheet::default();
        let mut values = values.iter();
        let current_date = chrono::Utc::now();
        let year = current_date.year();
        let month = current_date.month();
        let day = current_date.day();
        let term_parser = NaiveDate::parse_from_str;
        // Don't use unwrap
        let term = term_parser(term, "%m/%d/%Y").unwrap();

        if let Some(date) = chrono::NaiveDate::from_ymd_opt(year, month, day) {
            for title in titles.iter() {
                match title.as_ref() {
                    "Total Assets" => {
                        balance_sheet.total_assets = values.next().cloned();
                    }
                    "Total Liabilities Net Minority Interest" => {
                        balance_sheet.total_liabilities_net_minority_interest =
                            values.next().cloned();
                    }
                    "Total Equity Gross Minority Interest" => {
                        balance_sheet.total_equity_gross_minority_interest = values.next().cloned();
                    }
                    "Total Capitalization" => {
                        balance_sheet.total_capitalization = values.next().cloned();
                    }
                    "Preferred Stock Equity" => {
                        balance_sheet.preferred_stock_equity = values.next().cloned();
                    }
                    "Common Stock Equity" => {
                        balance_sheet.common_stock_equity = values.next().cloned();
                    }
                    "Net Tangible Assets" => {
                        balance_sheet.net_tangible_assets = values.next().cloned();
                    }
                    "Invested Capital" => {
                        balance_sheet.invested_capital = values.next().cloned();
                    }
                    "Tangible Book Value" => {
                        balance_sheet.tangible_book_value = values.next().cloned();
                    }
                    "Total Debt" => {
                        balance_sheet.total_debt = values.next().cloned();
                    }
                    "Net Debt" => {
                        balance_sheet.net_debt = values.next().cloned();
                    }
                    "Share Issued" => {
                        balance_sheet.share_issued = values.next().cloned();
                    }
                    "Ordinary Shares Number" => {
                        balance_sheet.ordinary_shares_number = values.next().cloned();
                    }
                    "Preferred Shares Number" => {
                        balance_sheet.preferred_shares_number = values.next().cloned();
                    }
                    "Treasury Shares Number" => {
                        balance_sheet.treasury_shares_number = values.next().cloned();
                    }
                    "Working Capital" => {
                        balance_sheet.working_capital = values.next().cloned();
                    }
                    "Capital Lease Obligations" => {
                        balance_sheet.capital_lease_obligations = values.next().cloned();
                    }
                    &_ => {
                        println!(">>>>>>>> New field (Balance Sheet): {title}");
                    }
                }
            }
            balance_sheet.symbol = symbol.to_string();
            balance_sheet.term = term;
            balance_sheet.filed = date;
            balance_sheet.version = BALANCE_SHEETS_SCHEMA_VERSION;
            balance_sheet
        } else {
            // TODO: do not panic
            panic!("Date error");
        }
    }

    pub fn hash(&self) -> String {
        let mut hasher = blake3::Hasher::new();

        hasher.update(self.symbol.to_string().as_bytes());
        hasher.update(self.term.to_string().as_bytes());

        if let Some(total_assets) = &self.total_assets {
            hasher.update(total_assets.as_bytes());
        }

        if let Some(total_liabilities_net_minority_interest) =
            &self.total_liabilities_net_minority_interest
        {
            hasher.update(total_liabilities_net_minority_interest.as_bytes());
        }

        if let Some(total_equity_gross_minority_interest) =
            &self.total_equity_gross_minority_interest
        {
            hasher.update(total_equity_gross_minority_interest.as_bytes());
        }

        if let Some(total_capitalization) = &self.total_capitalization {
            hasher.update(total_capitalization.as_bytes());
        }

        if let Some(preferred_stock_equity) = &self.preferred_stock_equity {
            hasher.update(preferred_stock_equity.as_bytes());
        }

        if let Some(common_stock_equity) = &self.common_stock_equity {
            hasher.update(common_stock_equity.as_bytes());
        }

        if let Some(net_tangible_assets) = &self.net_tangible_assets {
            hasher.update(net_tangible_assets.as_bytes());
        }

        if let Some(invested_capital) = &self.invested_capital {
            hasher.update(invested_capital.as_bytes());
        }

        if let Some(tangible_book_value) = &self.tangible_book_value {
            hasher.update(tangible_book_value.as_bytes());
        }

        if let Some(total_debt) = &self.total_debt {
            hasher.update(total_debt.as_bytes());
        }

        if let Some(net_debt) = &self.net_debt {
            hasher.update(net_debt.as_bytes());
        }

        if let Some(share_issued) = &self.share_issued {
            hasher.update(share_issued.as_bytes());
        }

        if let Some(ordinary_shares_number) = &self.ordinary_shares_number {
            hasher.update(ordinary_shares_number.as_bytes());
        }

        if let Some(preferred_shares_number) = &self.preferred_shares_number {
            hasher.update(preferred_shares_number.as_bytes());
        }

        if let Some(treasury_shares_number) = &self.treasury_shares_number {
            hasher.update(treasury_shares_number.as_bytes());
        }

        if let Some(working_capital) = &self.working_capital {
            hasher.update(working_capital.as_bytes());
        }

        if let Some(capital_lease_obligations) = &self.capital_lease_obligations {
            hasher.update(capital_lease_obligations.as_bytes());
        }

        let hash = hasher.finalize();
        BASE64_STANDARD.encode(hash.as_bytes())
    }
}
impl Spider for BalanceSheet {
    fn fetch(symbol: &str) -> Result<String, reqwest::Error> {
        let url = format!("{YAHOO_ROOT}/quote/{symbol}/balance-sheet");
        println!("---> Fetching Balance Sheet for: {symbol}");
        println!("---> {url}");
        let client = reqwest::blocking::Client::builder()
            .user_agent(USER_AGENT)
            .build()?;
        let html = client.get(url).send()?.text()?;
        Ok(html)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_balance_sheets_standard_bank() {
        let symbol = "SBKP.JO";
        let html = BalanceSheet::fetch(symbol).unwrap();
        let balance_sheets: Vec<BalanceSheet> = BalanceSheet::parse(&html, symbol);

        assert_eq!(balance_sheets.len(), 5);
        assert_eq!(
            balance_sheets[0].total_assets,
            Some("3,065,745,000.00".to_string())
        );
        assert_eq!(
            balance_sheets[0].total_liabilities_net_minority_interest,
            Some("2,788,825,000.00".to_string())
        );
        assert_eq!(
            balance_sheets[0].total_equity_gross_minority_interest,
            Some("276,920,000.00".to_string())
        );
        assert_eq!(
            balance_sheets[0].total_capitalization,
            Some("393,537,000.00".to_string())
        );
        assert_eq!(
            balance_sheets[0].preferred_stock_equity,
            Some("5,503,000.00".to_string())
        );
        assert_eq!(
            balance_sheets[0].common_stock_equity,
            Some("255,109,000.00".to_string())
        );
        assert_eq!(
            balance_sheets[0].net_tangible_assets,
            Some("247,889,000.00".to_string())
        );
        assert_eq!(
            balance_sheets[0].invested_capital,
            Some("388,034,000.00".to_string())
        );
        assert_eq!(
            balance_sheets[0].tangible_book_value,
            Some("242,386,000.00".to_string())
        );
        assert_eq!(
            balance_sheets[0].total_debt,
            Some("136,639,000.00".to_string())
        );
        assert_eq!(balance_sheets[0].net_debt, Some("--".to_string()));
        assert_eq!(
            balance_sheets[0].share_issued,
            Some("1,675,775.23".to_string())
        );
        assert_eq!(
            balance_sheets[0].ordinary_shares_number,
            Some("1,657,074.12".to_string())
        );
        assert_eq!(
            balance_sheets[0].preferred_shares_number,
            Some("60,982.25".to_string())
        );
        assert_eq!(
            balance_sheets[0].treasury_shares_number,
            Some("18,701.11".to_string())
        );

        assert_eq!(balance_sheets[0].working_capital, None);

        assert_eq!(balance_sheets[0].capital_lease_obligations, None);
    }

    // #[test]
    // fn test_multiple_balance_sheets() {
    //     let symbol = "avgo";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "kfy";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "bili";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "bbar";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "cepu";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "tgs";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "vrt";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "bma";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "mcw";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "cc";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "teo";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "rytm";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "hph";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "tal";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "ibrx";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "incy";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "ymm";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "cgnx";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "qrvo";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "pam";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);

    //     let symbol = "srpt";
    //     let html = BalanceSheet::fetch(symbol).unwrap();
    //     BalanceSheet::parse(&html, symbol);
    // }
}
