use crate::{Spider, USER_AGENT, YAHOO_ROOT};
use base64::prelude::*;
use chrono::{Datelike, NaiveDate};
#[cfg(feature = "postgres")]
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};

#[cfg(feature = "postgres")]
pub mod database;
pub const INCOME_STATEMENT_SCHEMA_VERSION: i16 = 0;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IncomeStatement {
    pub symbol: String,
    pub term: NaiveDate,
    pub total_revenue: String,
    pub income_from_associates_and_other_participating_interests: String,
    pub special_income_charges: String,
    pub other_non_operating_income_expenses: String,
    pub pretax_income: String,
    pub tax_provision: String,
    pub net_income_from_continuing_operation_net_minority_interest: String,
    pub diluted_ni_available_to_com_stockholders: String,
    pub net_from_continuing_and_discontinued_operation: String,
    pub normalized_income: String,
    pub reconciled_depreciation: String,
    pub total_unusual_items_excluding_goodwill: String,
    pub total_unusual_items: String,
    pub tax_rate_for_calcs: String,
    pub tax_effect_of_unusual_items: String,
    #[cfg(feature = "postgres")]
    pub filed: NaiveDate,
    pub version: i16,
}

impl IncomeStatement {
    /// Parse html code for income statements page
    pub fn parse(html: &str, symbol: &str) -> Vec<IncomeStatement> {
        let document = Html::parse_document(html);
        let rows = Selector::parse(".tableBody .row").unwrap();
        let mut year1 = vec![];
        let mut year2 = vec![];
        let mut year3 = vec![];
        let mut year4 = vec![];
        let mut terms = IncomeStatement::get_terms(&document).into_iter();
        let mut res = vec![];

        for row in document.select(&rows) {
            let columns_html = row.inner_html();
            let fragment = Html::parse_fragment(&columns_html);
            let columns = Selector::parse(".column").unwrap();

            for (column_count, column) in fragment.select(&columns).enumerate() {
                if column_count != 0 || column_count != 1 {
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
                    }
                }
            }
        }

        if let Some(term) = terms.next() {
            res.push(IncomeStatement::from_vec(year1, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(IncomeStatement::from_vec(year2, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(IncomeStatement::from_vec(year3, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(IncomeStatement::from_vec(year4, &term, symbol));
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

    /// Create income statement from a Vec<String>
    fn from_vec(values: Vec<String>, term: &str, symbol: &str) -> Self {
        let current_date = chrono::Utc::now();
        let year = current_date.year();
        let month = current_date.month();
        let day = current_date.day();
        let term_parser = NaiveDate::parse_from_str;
        // Don't use unwrap
        let term = term_parser(term, "%m/%d/%Y").unwrap();

        if let Some(date) = chrono::NaiveDate::from_ymd_opt(year, month, day) {
            Self {
                symbol: symbol.to_string(),
                term,
                total_revenue: values[0].clone(),
                income_from_associates_and_other_participating_interests: values[1].clone(),
                special_income_charges: values[2].clone(),
                other_non_operating_income_expenses: values[3].clone(),
                pretax_income: values[4].clone(),
                tax_provision: values[5].clone(),
                net_income_from_continuing_operation_net_minority_interest: values[6].clone(),
                diluted_ni_available_to_com_stockholders: values[7].clone(),
                net_from_continuing_and_discontinued_operation: values[8].clone(),
                normalized_income: values[9].clone(),
                reconciled_depreciation: values[10].clone(),
                total_unusual_items_excluding_goodwill: values[11].clone(),
                total_unusual_items: values[12].clone(),
                // TODO: Find a way to parse the 13th field
                tax_rate_for_calcs: values[14].clone(),
                tax_effect_of_unusual_items: values[15].clone(),
                filed: date,
                version: INCOME_STATEMENT_SCHEMA_VERSION,
            }
        } else {
            // TODO: do not panic
            panic!("Date error");
        }
    }

    pub fn hash(&self) -> String {
        let mut hasher = blake3::Hasher::new();

        hasher.update(self.symbol.to_string().as_bytes());
        hasher.update(self.term.to_string().as_bytes());
        hasher.update(self.total_revenue.as_bytes());
        hasher.update(
            self.income_from_associates_and_other_participating_interests
                .as_bytes(),
        );
        hasher.update(self.special_income_charges.as_bytes());
        hasher.update(self.other_non_operating_income_expenses.as_bytes());
        hasher.update(self.pretax_income.as_bytes());
        hasher.update(self.tax_provision.as_bytes());
        hasher.update(
            self.net_income_from_continuing_operation_net_minority_interest
                .as_bytes(),
        );
        hasher.update(self.diluted_ni_available_to_com_stockholders.as_bytes());
        hasher.update(
            self.net_from_continuing_and_discontinued_operation
                .as_bytes(),
        );
        hasher.update(self.normalized_income.as_bytes());
        hasher.update(self.reconciled_depreciation.as_bytes());
        hasher.update(self.total_unusual_items_excluding_goodwill.as_bytes());
        hasher.update(self.total_unusual_items.as_bytes());
        hasher.update(self.tax_rate_for_calcs.as_bytes());
        hasher.update(self.tax_effect_of_unusual_items.as_bytes());

        let hash = hasher.finalize();
        BASE64_STANDARD.encode(hash.as_bytes())
    }
}
impl Spider for IncomeStatement {
    /// Download income statements HTML
    fn fetch(symbol: &str) -> Result<String, reqwest::Error> {
        let url = format!("{YAHOO_ROOT}/quote/{symbol}/financials");
        println!("---> Fetching Income Statements for: {symbol}");
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
    fn test_income_statements_fetch_parse() {
        let symbol = "SBKP.JO";
        let html = IncomeStatement::fetch(symbol).unwrap();
        let income_statements: Vec<IncomeStatement> = IncomeStatement::parse(&html, symbol);

        assert_eq!(income_statements.len(), 4);
        assert_eq!(
            income_statements[0].total_revenue,
            "189,561,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].income_from_associates_and_other_participating_interests,
            "1,648,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].special_income_charges,
            "-4,533,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].other_non_operating_income_expenses,
            "23,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].pretax_income,
            "66,368,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].tax_provision,
            "16,065,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].net_income_from_continuing_operation_net_minority_interest,
            "44,211,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].diluted_ni_available_to_com_stockholders,
            "44,211,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].net_from_continuing_and_discontinued_operation,
            "45,973,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].normalized_income,
            "48,032,486.00".to_string()
        );
        assert_eq!(
            income_statements[0].reconciled_depreciation,
            "7,303,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].total_unusual_items_excluding_goodwill,
            "45,973,000.00".to_string()
        );
        assert_eq!(
            income_statements[0].total_unusual_items,
            "-2,717,000.00".to_string()
        );

        assert_eq!(income_statements[0].tax_rate_for_calcs, "0.00".to_string());
        assert_eq!(
            income_statements[0].tax_effect_of_unusual_items,
            "-657,514.00".to_string()
        );
    }
}
