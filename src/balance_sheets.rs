use crate::{Spider, USER_AGENT, YAHOO_ROOT};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BalanceSheet {
    pub total_assets: String,
    pub total_liabilities_net_minority_interest: String,
    pub total_equity_gross_minority_interest: String,
    pub total_capitalization: String,
    pub preferred_stock_equity: String,
    pub common_stock_equity: String,
    pub net_tangible_assets: String,
    pub invested_capital: String,
    pub tangible_book_value: String,
    pub total_debt: String,
    pub net_debt: String,
    pub share_issued: String,
    pub ordinary_shares_number: String,
    pub preferred_shares_number: String,
    pub treasury_shares_number: String,
}

impl BalanceSheet {
    pub fn parse(html: &str) -> Vec<BalanceSheet> {
        let document = Html::parse_document(html);
        let rows = Selector::parse(".tableBody .row").unwrap();
        let mut year1 = vec![];
        let mut year2 = vec![];
        let mut year3 = vec![];
        let mut year4 = vec![];
        let mut year5 = vec![];

        for row in document.select(&rows) {
            let columns_html = row.inner_html();
            let fragment = Html::parse_fragment(&columns_html);
            let columns = Selector::parse(".column").unwrap();

            for (column_count, column) in fragment.select(&columns).enumerate() {
                if column_count != 0 {
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

        let year1 = BalanceSheet::from_vec(year1);
        let year2 = BalanceSheet::from_vec(year2);
        let year3 = BalanceSheet::from_vec(year3);
        let year4 = BalanceSheet::from_vec(year4);
        let year5 = BalanceSheet::from_vec(year5);

        vec![year1, year2, year3, year4, year5]
    }

    fn from_vec(values: Vec<String>) -> Self {
        Self {
            total_assets: values[0].clone(),
            total_liabilities_net_minority_interest: values[1].clone(),
            total_equity_gross_minority_interest: values[2].clone(),
            total_capitalization: values[3].clone(),
            preferred_stock_equity: values[4].clone(),
            common_stock_equity: values[5].clone(),
            net_tangible_assets: values[6].clone(),
            invested_capital: values[7].clone(),
            tangible_book_value: values[8].clone(),
            total_debt: values[9].clone(),
            net_debt: values[10].clone(),
            share_issued: values[11].clone(),
            ordinary_shares_number: values[12].clone(),
            preferred_shares_number: values[13].clone(),
            treasury_shares_number: values[14].clone(),
        }
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
    fn test_balance_sheets() {
        let symbol = "SBKP.JO";
        let html = BalanceSheet::fetch(symbol).unwrap();
        let balance_sheets: Vec<BalanceSheet> = BalanceSheet::parse(&html);

        assert_eq!(balance_sheets.len(), 5);
        assert_eq!(
            balance_sheets[0].total_assets,
            "3,065,745,000.00".to_string()
        );
        assert_eq!(
            balance_sheets[0].total_liabilities_net_minority_interest,
            "2,788,825,000.00".to_string()
        );
        assert_eq!(
            balance_sheets[0].total_equity_gross_minority_interest,
            "276,920,000.00".to_string()
        );
        assert_eq!(
            balance_sheets[0].total_capitalization,
            "393,537,000.00".to_string()
        );
        assert_eq!(
            balance_sheets[0].preferred_stock_equity,
            "5,503,000.00".to_string()
        );
        assert_eq!(
            balance_sheets[0].common_stock_equity,
            "255,109,000.00".to_string()
        );
        assert_eq!(
            balance_sheets[0].net_tangible_assets,
            "247,889,000.00".to_string()
        );
        assert_eq!(
            balance_sheets[0].invested_capital,
            "388,034,000.00".to_string()
        );
        assert_eq!(
            balance_sheets[0].tangible_book_value,
            "242,386,000.00".to_string()
        );
        assert_eq!(balance_sheets[0].total_debt, "136,639,000.00".to_string());
        assert_eq!(balance_sheets[0].net_debt, "--".to_string());
        assert_eq!(balance_sheets[0].share_issued, "1,675,775.23".to_string());
        assert_eq!(
            balance_sheets[0].ordinary_shares_number,
            "1,657,074.12".to_string()
        );
        assert_eq!(
            balance_sheets[0].preferred_shares_number,
            "60,982.25".to_string()
        );
        assert_eq!(
            balance_sheets[0].treasury_shares_number,
            "18,701.11".to_string()
        );
    }
}
