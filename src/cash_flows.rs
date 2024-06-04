use crate::{Spider, USER_AGENT, YAHOO_ROOT};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CashFlow {
    pub cash_flows_from_used_in_operating_activities_direct: String,
    pub operating_cash_flow: String,
    pub investing_cash_flow: String,
    pub financing_cash_flow: String,
    pub end_cash_position: String,
    pub capital_expenditure: String,
    pub issuance_of_capital_stock: String,
    pub issuance_of_debt: String,
    pub repayment_of_debt: String,
    pub repurchase_of_capital_stock: String,
    pub free_cash_flow: String,
}

impl CashFlow {
    pub fn parse(html: &str) -> Vec<CashFlow> {
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
                    } else if column_count == 6 {
                        year5.push(t);
                    }
                }
            }
        }

        let year1 = CashFlow::from_vec(year1);
        let year2 = CashFlow::from_vec(year2);
        let year3 = CashFlow::from_vec(year3);
        let year4 = CashFlow::from_vec(year4);
        let year5 = CashFlow::from_vec(year5);

        vec![year1, year2, year3, year4, year5]
    }

    fn from_vec(values: Vec<String>) -> Self {
        Self {
            cash_flows_from_used_in_operating_activities_direct: values[0].clone(),
            operating_cash_flow: values[1].clone(),
            investing_cash_flow: values[2].clone(),
            financing_cash_flow: values[3].clone(),
            end_cash_position: values[4].clone(),
            capital_expenditure: values[5].clone(),
            issuance_of_capital_stock: values[6].clone(),
            issuance_of_debt: values[7].clone(),
            repayment_of_debt: values[8].clone(),
            repurchase_of_capital_stock: values[9].clone(),
            free_cash_flow: values[10].clone(),
        }
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
        let cash_flows: Vec<CashFlow> = CashFlow::parse(&html);

        assert_eq!(cash_flows.len(), 5);

        assert_eq!(
            cash_flows[0].cash_flows_from_used_in_operating_activities_direct,
            "48,698,000.00".to_string()
        );
        assert_eq!(
            cash_flows[0].operating_cash_flow,
            "48,698,000.00".to_string()
        );
        assert_eq!(
            cash_flows[0].investing_cash_flow,
            "-6,051,000.00".to_string()
        );
        assert_eq!(
            cash_flows[0].financing_cash_flow,
            "-26,796,000.00".to_string()
        );
        assert_eq!(
            cash_flows[0].end_cash_position,
            "205,189,000.00".to_string()
        );
        assert_eq!(
            cash_flows[0].capital_expenditure,
            "-6,339,000.00".to_string()
        );
        assert_eq!(
            cash_flows[0].issuance_of_capital_stock,
            "40,000.00".to_string()
        );
        assert_eq!(cash_flows[0].issuance_of_debt, "5,639,000.00".to_string());
        assert_eq!(cash_flows[0].repayment_of_debt, "-5,900,000.00".to_string());
        assert_eq!(
            cash_flows[0].repurchase_of_capital_stock,
            "-443,000.00".to_string()
        );
        assert_eq!(cash_flows[0].free_cash_flow, "42,359,000.00".to_string());
    }
}
