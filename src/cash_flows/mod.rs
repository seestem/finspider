use crate::{Spider, USER_AGENT, YAHOO_ROOT};
use base64::prelude::*;
use chrono::{Datelike, NaiveDate};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};

#[cfg(feature = "postgres")]
pub mod database;

pub const CASH_FLOWS_SCHEMA_VERSION: i16 = 0;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct CashFlow {
    pub symbol: String,
    pub term: NaiveDate,
    pub cash_flows_from_used_in_operating_activities_direct: Option<String>,
    pub operating_cash_flow: Option<String>,
    pub investing_cash_flow: Option<String>,
    pub financing_cash_flow: Option<String>,
    pub end_cash_position: Option<String>,
    pub capital_expenditure: Option<String>,
    pub issuance_of_capital_stock: Option<String>,
    pub issuance_of_debt: Option<String>,
    pub repayment_of_debt: Option<String>,
    pub repurchase_of_capital_stock: Option<String>,
    pub free_cash_flow: Option<String>,
    pub income_tax_paid_supplemental_data: Option<String>,
    pub interest_paid_supplemental_data: Option<String>,
    pub other_cash_adjustment_inside_change_in_cash: Option<String>,
    #[cfg(feature = "postgres")]
    pub filed: NaiveDate,
    pub version: i16,
}

impl CashFlow {
    pub fn parse(html: &str, symbol: &str) -> Vec<CashFlow> {
        let document = Html::parse_document(html);
        let rows = Selector::parse(".tableBody .row").unwrap();
        let mut year1 = vec![];
        let mut year2 = vec![];
        let mut year3 = vec![];
        let mut year4 = vec![];
        let mut year5 = vec![];
        let mut titles = vec![];
        let mut terms = CashFlow::get_terms(&document).into_iter();
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
                } else if column_count != 0 || column_count != 1 {
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

                    if column_count == 2 {
                        year1.push(t);
                    } else if column_count == 3 {
                        year2.push(t);
                    } else if column_count == 4 {
                        year3.push(t);
                    } else if column_count == 5 {
                        year4.push(t);
                    } else if column_count == 6 {
                        year5.push(t);
                    }
                }
            }
        }

        if let Some(term) = terms.next() {
            res.push(CashFlow::from_vec(&titles, year1, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(CashFlow::from_vec(&titles, year2, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(CashFlow::from_vec(&titles, year3, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(CashFlow::from_vec(&titles, year4, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(CashFlow::from_vec(&titles, year5, &term, symbol));
        };

        res
    }

    fn get_terms(html: &Html) -> Vec<String> {
        let headers = Selector::parse(".tableHeader .column").unwrap();
        let mut titles = vec![];

        for (header_count, header) in html.select(&headers).enumerate() {
            if header_count != 0 && header_count != 1 {
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
        let mut cash_flow = CashFlow::default();
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
                    "Cash Flows from Used in Operating Activities Direct" => {
                        cash_flow.cash_flows_from_used_in_operating_activities_direct =
                            values.next().cloned();
                    }
                    "Operating Cash Flow" => {
                        cash_flow.operating_cash_flow = values.next().cloned();
                    }
                    "Investing Cash Flow" => {
                        cash_flow.investing_cash_flow = values.next().cloned();
                    }
                    "Financing Cash Flow" => {
                        cash_flow.financing_cash_flow = values.next().cloned();
                    }
                    "End Cash Position" => {
                        cash_flow.end_cash_position = values.next().cloned();
                    }

                    "Capital Expenditure" => {
                        cash_flow.capital_expenditure = values.next().cloned();
                    }

                    "Issuance of Capital Stock" => {
                        cash_flow.issuance_of_capital_stock = values.next().cloned();
                    }
                    "Issuance of Debt" => {
                        cash_flow.issuance_of_debt = values.next().cloned();
                    }
                    "Repayment of Debt" => {
                        cash_flow.repayment_of_debt = values.next().cloned();
                    }
                    "Repurchase of Capital Stock" => {
                        cash_flow.repurchase_of_capital_stock = values.next().cloned();
                    }
                    "Free Cash Flow" => {
                        cash_flow.free_cash_flow = values.next().cloned();
                    }
                    "Income Tax Paid Supplemental Data" => {
                        cash_flow.income_tax_paid_supplemental_data = values.next().cloned();
                    }
                    "Interest Paid Supplemental Data" => {
                        cash_flow.interest_paid_supplemental_data = values.next().cloned();
                    }
                    "Other Cash Adjustment Inside Change in Cash" => {
                        cash_flow.other_cash_adjustment_inside_change_in_cash =
                            values.next().cloned();
                    }
                    &_ => {
                        println!(">>>>>>>> New field (Cash Flow): {title}");
                    }
                }
            }
            cash_flow.symbol = symbol.to_string();
            cash_flow.term = term;
            cash_flow.filed = date;
            cash_flow.version = CASH_FLOWS_SCHEMA_VERSION;
            cash_flow
        } else {
            // TODO: do not panic
            panic!("Date error");
        }
    }

    pub fn hash(&self) -> String {
        let mut hasher = blake3::Hasher::new();

        hasher.update(self.symbol.to_string().as_bytes());
        hasher.update(self.term.to_string().as_bytes());

        if let Some(cash_flows_from_used_in_operating_activities_direct) =
            &self.cash_flows_from_used_in_operating_activities_direct
        {
            hasher.update(
                cash_flows_from_used_in_operating_activities_direct
                    .to_string()
                    .as_bytes(),
            );
        }
        if let Some(operating_cash_flow) = &self.operating_cash_flow {
            hasher.update(operating_cash_flow.to_string().as_bytes());
        }
        if let Some(investing_cash_flow) = &self.investing_cash_flow {
            hasher.update(investing_cash_flow.to_string().as_bytes());
        }
        if let Some(financing_cash_flow) = &self.financing_cash_flow {
            hasher.update(financing_cash_flow.to_string().as_bytes());
        }
        if let Some(end_cash_position) = &self.end_cash_position {
            hasher.update(end_cash_position.to_string().as_bytes());
        }
        if let Some(capital_expenditure) = &self.capital_expenditure {
            hasher.update(capital_expenditure.to_string().as_bytes());
        }
        if let Some(issuance_of_capital_stock) = &self.issuance_of_capital_stock {
            hasher.update(issuance_of_capital_stock.to_string().as_bytes());
        }
        if let Some(issuance_of_debt) = &self.issuance_of_debt {
            hasher.update(issuance_of_debt.to_string().as_bytes());
        }
        if let Some(repayment_of_debt) = &self.repayment_of_debt {
            hasher.update(repayment_of_debt.to_string().as_bytes());
        }
        if let Some(repurchase_of_capital_stock) = &self.repurchase_of_capital_stock {
            hasher.update(repurchase_of_capital_stock.to_string().as_bytes());
        }
        if let Some(free_cash_flow) = &self.free_cash_flow {
            hasher.update(free_cash_flow.to_string().as_bytes());
        }
        if let Some(income_tax_paid_supplemental_data) = &self.income_tax_paid_supplemental_data {
            hasher.update(income_tax_paid_supplemental_data.to_string().as_bytes());
        }
        if let Some(interest_paid_supplemental_data) = &self.interest_paid_supplemental_data {
            hasher.update(interest_paid_supplemental_data.to_string().as_bytes());
        }
        if let Some(other_cash_adjustment_inside_change_in_cash) =
            &self.other_cash_adjustment_inside_change_in_cash
        {
            hasher.update(
                other_cash_adjustment_inside_change_in_cash
                    .to_string()
                    .as_bytes(),
            );
        }
        let hash = hasher.finalize();
        BASE64_STANDARD.encode(hash.as_bytes())
    }
}
impl Spider for CashFlow {
    fn fetch(symbol: &str) -> Result<String, reqwest::Error> {
        let url = format!("{YAHOO_ROOT}/quote/{symbol}/cash-flow");
        println!("---> Fetching Cash Flow for: {symbol}");
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
    fn test_cash_flows() {
        let symbol = "SBKP.JO";
        let html = CashFlow::fetch(symbol).unwrap();
        let cash_flows: Vec<CashFlow> = CashFlow::parse(&html, symbol);

        assert_eq!(cash_flows.len(), 5);

        assert_eq!(
            cash_flows[0].cash_flows_from_used_in_operating_activities_direct,
            Some("48,698,000.00".to_string())
        );
        assert_eq!(
            cash_flows[0].operating_cash_flow,
            Some("48,698,000.00".to_string())
        );
        assert_eq!(
            cash_flows[0].investing_cash_flow,
            Some("-6,051,000.00".to_string())
        );
        assert_eq!(
            cash_flows[0].financing_cash_flow,
            Some("-26,796,000.00".to_string())
        );
        assert_eq!(
            cash_flows[0].end_cash_position,
            Some("205,189,000.00".to_string())
        );
        assert_eq!(
            cash_flows[0].capital_expenditure,
            Some("-6,339,000.00".to_string())
        );
        assert_eq!(
            cash_flows[0].issuance_of_capital_stock,
            Some("40,000.00".to_string())
        );
        assert_eq!(
            cash_flows[0].issuance_of_debt,
            Some("5,639,000.00".to_string())
        );
        assert_eq!(
            cash_flows[0].repayment_of_debt,
            Some("-5,900,000.00".to_string())
        );
        assert_eq!(
            cash_flows[0].repurchase_of_capital_stock,
            Some("-443,000.00".to_string())
        );
        assert_eq!(
            cash_flows[0].free_cash_flow,
            Some("42,359,000.00".to_string())
        );
    }

    // #[test]
    // fn test_multiple_cash_flows() {
    //     let symbol = "avgo";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "kfy";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "bili";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "bbar";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "cepu";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "tgs";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "vrt";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "bma";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "mcw";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "cc";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "teo";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "rytm";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "hph";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "tal";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "ibrx";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "incy";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "ymm";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "cgnx";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "qrvo";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "pam";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);

    //     let symbol = "srpt";
    //     let html = CashFlow::fetch(symbol).unwrap();
    //     CashFlow::parse(&html, symbol);
    // }
}
