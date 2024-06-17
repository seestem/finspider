use crate::{Spider, USER_AGENT, YAHOO_ROOT};
use base64::prelude::*;
use chrono::{Datelike, NaiveDate};
#[cfg(feature = "postgres")]
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};

#[cfg(feature = "postgres")]
pub mod database;
pub const INCOME_STATEMENT_SCHEMA_VERSION: i16 = 0;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct IncomeStatement {
    pub symbol: String,
    pub term: NaiveDate,
    pub total_revenue: Option<String>,
    pub income_from_associates_and_other_participating_interests: Option<String>,
    pub special_income_charges: Option<String>,
    pub other_non_operating_income_expenses: Option<String>,
    pub pretax_income: Option<String>,
    pub tax_provision: Option<String>,
    pub net_income_common_stockholders: Option<String>,
    pub net_income_from_continuing_operation_net_minority_interest: Option<String>,
    pub diluted_ni_available_to_com_stockholders: Option<String>,
    pub net_from_continuing_and_discontinued_operation: Option<String>,
    pub normalized_income: Option<String>,
    pub reconciled_depreciation: Option<String>,
    pub total_unusual_items_excluding_goodwill: Option<String>,
    pub total_unusual_items: Option<String>,
    pub tax_rate_for_calcs: Option<String>,
    pub tax_effect_of_unusual_items: Option<String>,
    pub cost_of_revenue: Option<String>,
    pub gross_profit: Option<String>,
    pub operating_expense: Option<String>,
    pub operating_income: Option<String>,
    pub net_non_operating_interest_income_expense: Option<String>,
    pub other_income_expense: Option<String>,
    pub basic_eps: Option<String>,
    pub diluted_eps: Option<String>,
    pub basic_average_shares: Option<String>,
    pub diluted_average_shares: Option<String>,
    pub total_operating_income_as_reported: Option<String>,
    pub total_expenses: Option<String>,
    pub interest_income: Option<String>,
    pub interest_expense: Option<String>,
    pub net_interest_income: Option<String>,
    pub ebit: Option<String>,
    pub ebitda: Option<String>,
    pub reconciled_cost_of_revenue: Option<String>,
    pub normalized_ebitda: Option<String>,
    pub average_dilution_earnings: Option<String>,
    pub credit_losses_provision: Option<String>,
    pub non_interest_expense: Option<String>,
    pub rent_expense_supplemental: Option<String>,
    pub interest_income_after_provision_for_loan_loss: Option<String>,
    pub total_money_market_investments: Option<String>,
    pub earnings_from_equity_interest_net_of_tax: Option<String>,
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
        let mut titles = vec![];
        let mut terms = IncomeStatement::get_terms(&document).into_iter();
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
                    }
                }
            }
        }

        if let Some(term) = terms.next() {
            res.push(IncomeStatement::from_vec(&titles, year1, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(IncomeStatement::from_vec(&titles, year2, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(IncomeStatement::from_vec(&titles, year3, &term, symbol));
        };

        if let Some(term) = terms.next() {
            res.push(IncomeStatement::from_vec(&titles, year4, &term, symbol));
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
    fn from_vec(titles: &[String], values: Vec<String>, term: &str, symbol: &str) -> Self {
        let mut income_statement = IncomeStatement::default();
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
                    "Total Revenue" => {
                        income_statement.total_revenue = values.next().cloned();
                    }
                    "Income from Associates & Other Participating Interests" => {
                        income_statement.income_from_associates_and_other_participating_interests =
                            values.next().cloned();
                    }
                    "Special Income Charges" => {
                        income_statement.special_income_charges = values.next().cloned();
                    }
                    "Other Non Operating Income Expenses" => {
                        income_statement.other_non_operating_income_expenses =
                            values.next().cloned();
                    }
                    "Pretax Income" => {
                        income_statement.pretax_income = values.next().cloned();
                    }
                    "Tax Provision" => {
                        income_statement.tax_provision = values.next().cloned();
                    }
                    "Net Income Common Stockholders" => {
                        income_statement.net_income_common_stockholders = values.next().cloned();
                    }
                    "Diluted NI Available to Com Stockholders" => {
                        income_statement.diluted_ni_available_to_com_stockholders =
                            values.next().cloned();
                    }
                    "Net Income from Continuing & Discontinued Operation" => {
                        income_statement.net_from_continuing_and_discontinued_operation =
                            values.next().cloned();
                    }
                    "Normalized Income" => {
                        income_statement.normalized_income = values.next().cloned();
                    }
                    "Reconciled Depreciation" => {
                        income_statement.reconciled_depreciation = values.next().cloned();
                    }
                    "Net Income from Continuing Operation Net Minority Interest" => {
                        income_statement
                            .net_income_from_continuing_operation_net_minority_interest =
                            values.next().cloned();
                    }
                    "Total Unusual Items Excluding Goodwill" => {
                        income_statement.total_unusual_items_excluding_goodwill =
                            values.next().cloned();
                    }
                    "Total Unusual Items" => {
                        income_statement.total_unusual_items = values.next().cloned();
                    }
                    "Tax Rate for Calcs" => {
                        income_statement.tax_rate_for_calcs = values.next().cloned();
                    }
                    "Tax Effect of Unusual Items" => {
                        income_statement.tax_effect_of_unusual_items = values.next().cloned();
                    }
                    "Cost of Revenue" => {
                        income_statement.cost_of_revenue = values.next().cloned();
                    }
                    "Gross Profit" => {
                        income_statement.gross_profit = values.next().cloned();
                    }
                    "Operating Expense" => {
                        income_statement.operating_expense = values.next().cloned();
                    }
                    "Operating Income" => {
                        income_statement.operating_expense = values.next().cloned();
                    }
                    "Net Non Operating Interest Income Expense" => {
                        income_statement.net_non_operating_interest_income_expense =
                            values.next().cloned();
                    }
                    "Other Income Expense" => {
                        income_statement.other_income_expense = values.next().cloned();
                    }
                    "Basic EPS" => {
                        income_statement.basic_eps = values.next().cloned();
                    }
                    "Diluted EPS" => {
                        income_statement.diluted_eps = values.next().cloned();
                    }
                    "Basic Average Shares" => {
                        income_statement.basic_average_shares = values.next().cloned();
                    }
                    "Diluted Average Shares" => {
                        income_statement.diluted_average_shares = values.next().cloned();
                    }
                    "Total Operating Income as Reported" => {
                        income_statement.total_operating_income_as_reported =
                            values.next().cloned();
                    }
                    "Total Expenses" => {
                        income_statement.total_expenses = values.next().cloned();
                    }
                    "Interest Income" => {
                        income_statement.interest_income = values.next().cloned();
                    }
                    "Interest Expense" => {
                        income_statement.interest_expense = values.next().cloned();
                    }
                    "Net Interest Income" => {
                        income_statement.net_interest_income = values.next().cloned();
                    }
                    "EBIT" => {
                        income_statement.ebit = values.next().cloned();
                    }
                    "EBITDA" => {
                        income_statement.ebitda = values.next().cloned();
                    }
                    "Reconciled Cost of Revenue" => {
                        income_statement.reconciled_cost_of_revenue = values.next().cloned();
                    }
                    "Normalized EBITDA" => {
                        income_statement.normalized_ebitda = values.next().cloned();
                    }
                    "Average Dilution Earnings" => {
                        income_statement.average_dilution_earnings = values.next().cloned();
                    }
                    "Credit Losses Provision" => {
                        income_statement.credit_losses_provision = values.next().cloned();
                    }
                    "Non Interest Expense" => {
                        income_statement.non_interest_expense = values.next().cloned();
                    }
                    "Rent Expense Supplemental" => {
                        income_statement.rent_expense_supplemental = values.next().cloned();
                    }
                    "Interest Income after Provision for Loan Loss" => {
                        income_statement.interest_income_after_provision_for_loan_loss =
                            values.next().cloned();
                    }
                    "Total Money Market Investments" => {
                        income_statement.total_money_market_investments = values.next().cloned();
                    }
                    "Earnings from Equity Interest Net of Tax" => {
                        income_statement.earnings_from_equity_interest_net_of_tax =
                            values.next().cloned();
                    }
                    &_ => {
                        println!(">>>>>>>> New field (Income Statements): {title}");
                    }
                }
            }

            income_statement.symbol = symbol.to_string();
            income_statement.term = term;
            income_statement.filed = date;
            income_statement.version = INCOME_STATEMENT_SCHEMA_VERSION;
            income_statement
        } else {
            // TODO: do not panic
            panic!("Date error");
        }
    }

    pub fn hash(&self) -> String {
        let mut hasher = blake3::Hasher::new();

        hasher.update(self.symbol.to_string().as_bytes());
        hasher.update(self.term.to_string().as_bytes());

        if let Some(total_revenue) = &self.total_revenue {
            hasher.update(total_revenue.as_bytes());
        }

        if let Some(income_from_associates_and_other_participating_interests) =
            &self.income_from_associates_and_other_participating_interests
        {
            hasher.update(income_from_associates_and_other_participating_interests.as_bytes());
        }

        if let Some(special_income_charges) = &self.special_income_charges {
            hasher.update(special_income_charges.as_bytes());
        }

        if let Some(other_non_operating_income_expenses) = &self.other_non_operating_income_expenses
        {
            hasher.update(other_non_operating_income_expenses.as_bytes());
        }

        if let Some(pretax_income) = &self.pretax_income {
            hasher.update(pretax_income.as_bytes());
        }

        if let Some(tax_provision) = &self.tax_provision {
            hasher.update(tax_provision.as_bytes());
        }

        if let Some(net_income_common_stockholders) = &self.net_income_common_stockholders {
            hasher.update(net_income_common_stockholders.as_bytes());
        }

        if let Some(net_income_from_continuing_operation_net_minority_interest) =
            &self.net_income_from_continuing_operation_net_minority_interest
        {
            hasher.update(net_income_from_continuing_operation_net_minority_interest.as_bytes());
        }

        if let Some(diluted_ni_available_to_com_stockholders) =
            &self.diluted_ni_available_to_com_stockholders
        {
            hasher.update(diluted_ni_available_to_com_stockholders.as_bytes());
        }

        if let Some(net_from_continuing_and_discontinued_operation) =
            &self.net_from_continuing_and_discontinued_operation
        {
            hasher.update(net_from_continuing_and_discontinued_operation.as_bytes());
        }

        if let Some(normalized_income) = &self.normalized_income {
            hasher.update(normalized_income.as_bytes());
        }

        if let Some(reconciled_depreciation) = &self.reconciled_depreciation {
            hasher.update(reconciled_depreciation.as_bytes());
        }

        if let Some(total_unusual_items_excluding_goodwill) =
            &self.total_unusual_items_excluding_goodwill
        {
            hasher.update(total_unusual_items_excluding_goodwill.as_bytes());
        }

        if let Some(total_unusual_items) = &self.total_unusual_items {
            hasher.update(total_unusual_items.as_bytes());
        }

        if let Some(tax_rate_for_calcs) = &self.tax_rate_for_calcs {
            hasher.update(tax_rate_for_calcs.as_bytes());
        }

        if let Some(tax_effect_of_unusual_items) = &self.tax_effect_of_unusual_items {
            hasher.update(tax_effect_of_unusual_items.as_bytes());
        }

        if let Some(cost_of_revenue) = &self.cost_of_revenue {
            hasher.update(cost_of_revenue.as_bytes());
        }

        if let Some(gross_profit) = &self.gross_profit {
            hasher.update(gross_profit.as_bytes());
        }

        if let Some(operating_expense) = &self.operating_expense {
            hasher.update(operating_expense.as_bytes());
        }

        if let Some(operating_income) = &self.operating_income {
            hasher.update(operating_income.as_bytes());
        }

        if let Some(net_non_operating_interest_income_expense) =
            &self.net_non_operating_interest_income_expense
        {
            hasher.update(net_non_operating_interest_income_expense.as_bytes());
        }

        if let Some(other_income_expense) = &self.other_income_expense {
            hasher.update(other_income_expense.as_bytes());
        }

        if let Some(basic_eps) = &self.basic_eps {
            hasher.update(basic_eps.as_bytes());
        }

        if let Some(diluted_eps) = &self.diluted_eps {
            hasher.update(diluted_eps.as_bytes());
        }

        if let Some(basic_average_shares) = &self.basic_average_shares {
            hasher.update(basic_average_shares.as_bytes());
        }

        if let Some(diluted_average_shares) = &self.diluted_average_shares {
            hasher.update(diluted_average_shares.as_bytes());
        }

        if let Some(total_operating_income_as_reported) = &self.total_operating_income_as_reported {
            hasher.update(total_operating_income_as_reported.as_bytes());
        }

        if let Some(total_expenses) = &self.total_expenses {
            hasher.update(total_expenses.as_bytes());
        }

        if let Some(interest_income) = &self.interest_income {
            hasher.update(interest_income.as_bytes());
        }

        if let Some(interest_expense) = &self.interest_expense {
            hasher.update(interest_expense.as_bytes());
        }

        if let Some(net_interest_income) = &self.net_interest_income {
            hasher.update(net_interest_income.as_bytes());
        }

        if let Some(ebit) = &self.ebit {
            hasher.update(ebit.as_bytes());
        }

        if let Some(ebitda) = &self.ebitda {
            hasher.update(ebitda.as_bytes());
        }

        if let Some(reconciled_cost_of_revenue) = &self.reconciled_cost_of_revenue {
            hasher.update(reconciled_cost_of_revenue.as_bytes());
        }

        if let Some(normalized_ebitda) = &self.normalized_ebitda {
            hasher.update(normalized_ebitda.as_bytes());
        }

        if let Some(average_dilution_earnings) = &self.average_dilution_earnings {
            hasher.update(average_dilution_earnings.as_bytes());
        }

        if let Some(credit_losses_provision) = &self.credit_losses_provision {
            hasher.update(credit_losses_provision.as_bytes());
        }

        if let Some(non_interest_expense) = &self.non_interest_expense {
            hasher.update(non_interest_expense.as_bytes());
        }

        if let Some(rent_expense_supplemental) = &self.rent_expense_supplemental {
            hasher.update(rent_expense_supplemental.as_bytes());
        }

        if let Some(interest_income_after_provision_for_loan_loss) =
            &self.interest_income_after_provision_for_loan_loss
        {
            hasher.update(interest_income_after_provision_for_loan_loss.as_bytes());
        }

        if let Some(total_money_market_investments) = &self.total_money_market_investments {
            hasher.update(total_money_market_investments.as_bytes());
        }

        if let Some(earnings_from_equity_interest_net_of_tax) =
            &self.earnings_from_equity_interest_net_of_tax
        {
            hasher.update(earnings_from_equity_interest_net_of_tax.as_bytes());
        }

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
            Some("189,561,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].income_from_associates_and_other_participating_interests,
            Some("1,648,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].special_income_charges,
            Some("-4,533,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].other_non_operating_income_expenses,
            Some("23,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].pretax_income,
            Some("66,368,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].tax_provision,
            Some("16,065,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].net_income_from_continuing_operation_net_minority_interest,
            Some("45,973,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].diluted_ni_available_to_com_stockholders,
            Some("44,211,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].net_from_continuing_and_discontinued_operation,
            Some("45,973,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].normalized_income,
            Some("48,032,486.00".to_string())
        );
        assert_eq!(
            income_statements[0].reconciled_depreciation,
            Some("7,303,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].total_unusual_items_excluding_goodwill,
            Some("-2,717,000.00".to_string())
        );
        assert_eq!(
            income_statements[0].total_unusual_items,
            Some("-2,717,000.00".to_string())
        );

        assert_eq!(
            income_statements[0].tax_rate_for_calcs,
            Some("0.00".to_string())
        );
        assert_eq!(
            income_statements[0].tax_effect_of_unusual_items,
            Some("-657,514.00".to_string())
        );
    }

    //#[test]
    //fn test_multiple_cash_flows() {
    // let symbol = "avgo";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "kfy";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "bili";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "bbar";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "cepu";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "tgs";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "vrt";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "bma";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "mcw";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "cc";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "teo";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "rytm";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    // let symbol = "hph";
    // let html = IncomeStatement::fetch(symbol).unwrap();
    // IncomeStatement::parse(&html, symbol);

    //     let symbol = "tal";
    //     let html = IncomeStatement::fetch(symbol).unwrap();
    //     IncomeStatement::parse(&html, symbol);

    //     let symbol = "ibrx";
    //     let html = IncomeStatement::fetch(symbol).unwrap();
    //     IncomeStatement::parse(&html, symbol);

    //     let symbol = "incy";
    //     let html = IncomeStatement::fetch(symbol).unwrap();
    //     IncomeStatement::parse(&html, symbol);

    //     let symbol = "ymm";
    //     let html = IncomeStatement::fetch(symbol).unwrap();
    //     IncomeStatement::parse(&html, symbol);

    //     let symbol = "cgnx";
    //     let html = IncomeStatement::fetch(symbol).unwrap();
    //     IncomeStatement::parse(&html, symbol);

    //     let symbol = "qrvo";
    //     let html = IncomeStatement::fetch(symbol).unwrap();
    //     IncomeStatement::parse(&html, symbol);

    //     let symbol = "pam";
    //     let html = IncomeStatement::fetch(symbol).unwrap();
    //     IncomeStatement::parse(&html, symbol);

    //     let symbol = "srpt";
    //     let html = IncomeStatement::fetch(symbol).unwrap();
    //     IncomeStatement::parse(&html, symbol);
    // }
}
