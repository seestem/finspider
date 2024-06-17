use super::IncomeStatement;
use crate::error::Error;
use postgres::Client;

/// Database management for income statements
pub struct IncomeStatementsDB;
impl IncomeStatementsDB {
    /// Initialize postgres cache
    pub fn init(client: &mut Client, table_name: &str, db_owner: &str) -> Result<(), Error> {
        let sql = format!(
            "
CREATE TABLE IF NOT EXISTS {table_name} (
	id serial NOT NULL,
        symbol varchar(80) NOT NULL,
        term date NOT NULL,
        total_revenue varchar(100),
        income_from_associates_and_other_participating_interests varchar(100),
        special_income_charges varchar(100),
        other_non_operating_income_expenses varchar(100),
        pretax_income varchar(100),
        tax_provision varchar(100),
        net_income_common_stockholders varchar(100),
        net_income_from_continuing_operation_net_minority_interest varchar(100),
        diluted_ni_available_to_com_stockholders varchar(100),
        net_from_continuing_and_discontinued_operation varchar(100),
        normalized_income varchar(100),
        reconciled_depreciation varchar(100),
        total_unusual_items_excluding_goodwill varchar(100),
        total_unusual_items varchar(100),
        tax_rate_for_calcs varchar(100),
        tax_effect_of_unusual_items varchar(100),
        cost_of_revenue varchar(100),
        gross_profit varchar(100),
        operating_expense varchar(100),
        operating_income varchar(100),
        net_non_operating_interest_income_expense varchar(100),
        other_income_expense varchar(100),
        basic_eps varchar(100),
        diluted_eps varchar(100),
        basic_average_shares varchar(100),
        diluted_average_shares varchar(100),
        total_operating_income_as_reported varchar(100),
        total_expenses varchar(100),
        interest_income varchar(100),
        interest_expense varchar(100),
        net_interest_income varchar(100),
        ebit varchar(100),
        ebitda varchar(100),
        reconciled_cost_of_revenue varchar(100),
        normalized_ebitda varchar(100),
        average_dilution_earnings varchar(100),
        credit_losses_provision varchar(100),
        non_interest_expense varchar(100),
        rent_expense_supplemental varchar(100),
        interest_income_after_provision_for_loan_loss varchar(100),
        total_money_market_investments varchar(100),
        earnings_from_equity_interest_net_of_tax varchar(100),
        filed date NOT NULL,
        hash text UNIQUE NOT NULL,
        version smallint NOT NULL
);
-- ddl-end --
ALTER TABLE {table_name} OWNER TO {db_owner};
-- ddl-end --"
        );
        client
            .batch_execute(&sql)
            .map_err(|_| Error::TableCreation)?;

        Ok(())
    }

    /// Save income statement in database
    pub fn save(
        client: &mut Client,
        table_name: &str,
        income_statement: IncomeStatement,
    ) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {table_name} (
        symbol,
        term,
        total_revenue,
        income_from_associates_and_other_participating_interests,
        special_income_charges,
        other_non_operating_income_expenses,
        pretax_income,
        tax_provision,
        net_income_common_stockholders,
        net_income_from_continuing_operation_net_minority_interest,
        diluted_ni_available_to_com_stockholders,
        net_from_continuing_and_discontinued_operation,
        normalized_income,
        reconciled_depreciation,
        total_unusual_items_excluding_goodwill,
        total_unusual_items,
        tax_rate_for_calcs,
        tax_effect_of_unusual_items,
        cost_of_revenue,
        gross_profit,
        operating_expense,
        operating_income,
        net_non_operating_interest_income_expense,
        other_income_expense,
        basic_eps,
        diluted_eps,
        basic_average_shares,
        diluted_average_shares,
        total_operating_income_as_reported,
        total_expenses,
        interest_income,
        interest_expense,
        net_interest_income,
        ebit,
        ebitda,
        normalized_ebitda,
        reconciled_cost_of_revenue,
        average_dilution_earnings,
        credit_losses_provision,
        non_interest_expense,
        rent_expense_supplemental,
        interest_income_after_provision_for_loan_loss,
        total_money_market_investments,
        earnings_from_equity_interest_net_of_tax,
        filed,
        hash,
        version
)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                         $12, $13, $14, $15, $16, $17, $18, $19, $20,
                         $21, $22, $23, $24, $25, $26, $27, $28, $29,
                         $30, $31, $32, $33, $34, $35, $36, $37, $38,
                         $39, $40, $41, $42, $43, $44, $45, $46, $47);"
        );

        client
            .execute(
                &sql,
                &[
                    &income_statement.symbol,
                    &income_statement.term,
                    &income_statement.total_revenue,
                    &income_statement.income_from_associates_and_other_participating_interests,
                    &income_statement.special_income_charges,
                    &income_statement.other_non_operating_income_expenses,
                    &income_statement.pretax_income,
                    &income_statement.tax_provision,
                    &income_statement.net_income_common_stockholders,
                    &income_statement.net_income_from_continuing_operation_net_minority_interest,
                    &income_statement.diluted_ni_available_to_com_stockholders,
                    &income_statement.net_from_continuing_and_discontinued_operation,
                    &income_statement.normalized_income,
                    &income_statement.reconciled_depreciation,
                    &income_statement.total_unusual_items_excluding_goodwill,
                    &income_statement.total_unusual_items,
                    &income_statement.tax_rate_for_calcs,
                    &income_statement.tax_effect_of_unusual_items,
                    &income_statement.cost_of_revenue,
                    &income_statement.gross_profit,
                    &income_statement.operating_expense,
                    &income_statement.operating_income,
                    &income_statement.net_non_operating_interest_income_expense,
                    &income_statement.other_income_expense,
                    &income_statement.basic_eps,
                    &income_statement.diluted_eps,
                    &income_statement.basic_average_shares,
                    &income_statement.diluted_average_shares,
                    &income_statement.total_operating_income_as_reported,
                    &income_statement.total_expenses,
                    &income_statement.interest_income,
                    &income_statement.interest_expense,
                    &income_statement.net_interest_income,
                    &income_statement.ebit,
                    &income_statement.ebitda,
                    &income_statement.reconciled_cost_of_revenue,
                    &income_statement.normalized_ebitda,
                    &income_statement.average_dilution_earnings,
                    &income_statement.credit_losses_provision,
                    &income_statement.non_interest_expense,
                    &income_statement.rent_expense_supplemental,
                    &income_statement.interest_income_after_provision_for_loan_loss,
                    &income_statement.total_money_market_investments,
                    &income_statement.earnings_from_equity_interest_net_of_tax,
                    &income_statement.filed,
                    &income_statement.hash(),
                    &income_statement.version,
                ],
            )
            .map_err(|_| Error::SQL)?;
        Ok(())
    }

    pub fn read(
        client: &mut Client,
        table_name: &str,
        hash: &str,
    ) -> Result<Option<IncomeStatement>, Error> {
        let sql = format!("SELECT * FROM {table_name} WHERE hash = $1");
        let row = client.query(&sql, &[&hash]).map_err(|_| Error::SQL)?;

        if row.is_empty() {
            Ok(None)
        } else {
            Ok(Some(IncomeStatement {
                symbol: row[0].get("symbol"),
                term: row[0].get("term"),
                total_revenue: row[0].get("total_revenue"),
                income_from_associates_and_other_participating_interests: row[0]
                    .get("income_from_associates_and_other_participating_interests"),
                special_income_charges: row[0].get("special_income_charges"),
                other_non_operating_income_expenses: row[0]
                    .get("other_non_operating_income_expenses"),
                pretax_income: row[0].get("pretax_income"),
                net_income_common_stockholders: row[0].get("net_income_common_stockholders"),
                tax_provision: row[0].get("tax_provision"),
                net_income_from_continuing_operation_net_minority_interest: row[0]
                    .get("net_income_from_continuing_operation_net_minority_interest"),
                diluted_ni_available_to_com_stockholders: row[0]
                    .get("diluted_ni_available_to_com_stockholders"),
                net_from_continuing_and_discontinued_operation: row[0]
                    .get("net_from_continuing_and_discontinued_operation"),
                normalized_income: row[0].get("normalized_income"),
                reconciled_depreciation: row[0].get("reconciled_depreciation"),
                total_unusual_items_excluding_goodwill: row[0]
                    .get("total_unusual_items_excluding_goodwill"),
                total_unusual_items: row[0].get("total_unusual_items"),
                tax_rate_for_calcs: row[0].get("tax_rate_for_calcs"),
                tax_effect_of_unusual_items: row[0].get("tax_effect_of_unusual_items"),
                cost_of_revenue: row[0].get("tax_effect_of_unusual_items"),
                gross_profit: row[0].get("gross_profit"),
                operating_expense: row[0].get("gross_profit"),
                operating_income: row[0].get("operating_income"),
                net_non_operating_interest_income_expense: row[0]
                    .get("net_non_operating_interest_income_expense"),
                other_income_expense: row[0].get("other_income_expense"),
                basic_eps: row[0].get("basic_eps"),
                diluted_eps: row[0].get("diluted_eps"),
                basic_average_shares: row[0].get("basic_average_shares"),
                diluted_average_shares: row[0].get("diluted_average_shares"),
                total_operating_income_as_reported: row[0]
                    .get("total_operating_income_as_reported"),
                total_expenses: row[0].get("total_expenses"),
                interest_income: row[0].get("interest_income"),
                interest_expense: row[0].get("interest_expense"),
                net_interest_income: row[0].get("net_interest_income"),
                ebit: row[0].get("ebit"),
                ebitda: row[0].get("ebitda"),
                reconciled_cost_of_revenue: row[0].get("reconciled_cost_of_revenue"),
                normalized_ebitda: row[0].get("normalized_ebitda"),
                average_dilution_earnings: row[0].get("average_dilution_earnings"),
                credit_losses_provision: row[0].get("credit_losses_provision"),
                non_interest_expense: row[0].get("non_interest_expense"),
                rent_expense_supplemental: row[0].get("rent_expense_supplemental"),
                interest_income_after_provision_for_loan_loss: row[0]
                    .get("interest_income_after_provision_for_loan_loss"),
                total_money_market_investments: row[0].get("total_money_market_investments"),
                earnings_from_equity_interest_net_of_tax: row[0]
                    .get("earnings_from_equity_interest_net_of_tax"),
                filed: row[0].get("filed"),
                version: row[0].get("version"),
            }))
        }
    }

    pub fn read_all(client: &mut Client, table_name: &str) -> Result<Vec<IncomeStatement>, Error> {
        let sql = format!("SELECT * FROM {table_name}");
        let row = client.query(&sql, &[]).map_err(|_| Error::SQL)?;
        let mut income_statements: Vec<IncomeStatement> = vec![];

        for r in &row {
            income_statements.push(IncomeStatement {
                symbol: r.get("symbol"),
                term: r.get("term"),
                total_revenue: r.get("total_revenue"),
                income_from_associates_and_other_participating_interests: r
                    .get("income_from_associates_and_other_participating_interests"),
                special_income_charges: r.get("special_income_charges"),
                other_non_operating_income_expenses: r.get("other_non_operating_income_expenses"),
                pretax_income: r.get("pretax_income"),
                net_income_common_stockholders: r.get("net_income_common_stockholders"),
                tax_provision: r.get("tax_provision"),
                net_income_from_continuing_operation_net_minority_interest: r
                    .get("net_income_from_continuing_operation_net_minority_interest"),
                diluted_ni_available_to_com_stockholders: r
                    .get("diluted_ni_available_to_com_stockholders"),
                net_from_continuing_and_discontinued_operation: r
                    .get("net_from_continuing_and_discontinued_operation"),
                normalized_income: r.get("normalized_income"),
                reconciled_depreciation: r.get("reconciled_depreciation"),
                total_unusual_items_excluding_goodwill: r
                    .get("total_unusual_items_excluding_goodwill"),
                total_unusual_items: r.get("total_unusual_items"),
                tax_rate_for_calcs: r.get("tax_rate_for_calcs"),
                tax_effect_of_unusual_items: r.get("tax_effect_of_unusual_items"),
                cost_of_revenue: r.get("tax_effect_of_unusual_items"),
                gross_profit: r.get("gross_profit"),
                operating_expense: r.get("operating_expense"),
                operating_income: r.get("operating_income"),
                net_non_operating_interest_income_expense: r
                    .get("net_non_operating_interest_income_expense"),
                other_income_expense: r.get("other_income_expense"),
                basic_eps: r.get("basic_eps"),
                diluted_eps: r.get("diluted_eps"),
                basic_average_shares: r.get("basic_average_shares"),
                diluted_average_shares: r.get("diluted_average_shares"),
                total_operating_income_as_reported: r.get("total_operating_income_as_reported"),
                total_expenses: r.get("total_expenses"),
                interest_income: r.get("interest_income"),
                interest_expense: r.get("interest_expense"),
                net_interest_income: r.get("net_interest_income"),
                ebit: r.get("ebit"),
                ebitda: r.get("ebitda"),
                reconciled_cost_of_revenue: r.get("reconciled_cost_of_revenue"),
                normalized_ebitda: r.get("normalized_ebitda"),
                average_dilution_earnings: r.get("average_dilution_earnings"),
                credit_losses_provision: r.get("credit_losses_provision"),
                non_interest_expense: r.get("non_interest_expense"),
                rent_expense_supplemental: r.get("rent_expense_supplemental"),
                interest_income_after_provision_for_loan_loss: r
                    .get("interest_income_after_provision_for_loan_loss"),
                total_money_market_investments: r.get("total_money_market_investments"),
                earnings_from_equity_interest_net_of_tax: r
                    .get("earnings_from_equity_interest_net_of_tax"),
                filed: r.get("filed"),
                version: r.get("version"),
            })
        }

        Ok(income_statements)
    }

    pub fn read_all_by_symbol(
        client: &mut Client,
        table_name: &str,
        symbol: &str,
    ) -> Result<Vec<IncomeStatement>, Error> {
        let sql = format!("SELECT * FROM {table_name} WHERE symbol = $1");
        let row = client.query(&sql, &[&symbol]).map_err(|_| Error::SQL)?;
        let mut income_statements: Vec<IncomeStatement> = vec![];

        for r in &row {
            income_statements.push(IncomeStatement {
                symbol: r.get("symbol"),
                term: r.get("term"),
                total_revenue: r.get("total_revenue"),
                income_from_associates_and_other_participating_interests: r
                    .get("income_from_associates_and_other_participating_interests"),
                special_income_charges: r.get("special_income_charges"),
                other_non_operating_income_expenses: r.get("other_non_operating_income_expenses"),
                pretax_income: r.get("pretax_income"),
                tax_provision: r.get("tax_provision"),
                net_income_common_stockholders: r.get("net_income_common_stockholders"),
                net_income_from_continuing_operation_net_minority_interest: r
                    .get("net_income_from_continuing_operation_net_minority_interest"),
                diluted_ni_available_to_com_stockholders: r
                    .get("diluted_ni_available_to_com_stockholders"),
                net_from_continuing_and_discontinued_operation: r
                    .get("net_from_continuing_and_discontinued_operation"),
                normalized_income: r.get("normalized_income"),
                reconciled_depreciation: r.get("reconciled_depreciation"),
                total_unusual_items_excluding_goodwill: r
                    .get("total_unusual_items_excluding_goodwill"),
                total_unusual_items: r.get("total_unusual_items"),
                tax_rate_for_calcs: r.get("tax_rate_for_calcs"),
                tax_effect_of_unusual_items: r.get("tax_effect_of_unusual_items"),
                cost_of_revenue: r.get("tax_effect_of_unusual_items"),
                gross_profit: r.get("gross_profit"),
                operating_expense: r.get("operating_expense"),
                operating_income: r.get("operating_income"),
                net_non_operating_interest_income_expense: r
                    .get("net_non_operating_interest_income_expense"),
                other_income_expense: r.get("other_income_expense"),
                basic_eps: r.get("basic_eps"),
                diluted_eps: r.get("diluted_eps"),
                basic_average_shares: r.get("basic_average_shares"),
                diluted_average_shares: r.get("diluted_average_shares"),
                total_operating_income_as_reported: r.get("total_operating_income_as_reported"),
                total_expenses: r.get("total_expenses"),
                interest_income: r.get("interest_income"),
                interest_expense: r.get("interest_expense"),
                net_interest_income: r.get("net_interest_income"),
                ebit: r.get("ebit"),
                ebitda: r.get("ebitda"),
                reconciled_cost_of_revenue: r.get("reconciled_cost_of_revenue"),
                normalized_ebitda: r.get("normalized_ebitda"),
                average_dilution_earnings: r.get("average_dilution_earnings"),
                credit_losses_provision: r.get("credit_losses_provision"),
                non_interest_expense: r.get("non_interest_expense"),
                rent_expense_supplemental: r.get("rent_expense_supplemental"),
                interest_income_after_provision_for_loan_loss: r
                    .get("interest_income_after_provision_for_loan_loss"),
                total_money_market_investments: r.get("total_money_market_investments"),
                earnings_from_equity_interest_net_of_tax: r
                    .get("earnings_from_equity_interest_net_of_tax"),
                filed: r.get("filed"),
                version: r.get("version"),
            })
        }

        Ok(income_statements)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use postgres::{Client, NoTls};
    use std::env;
    const TABLE: &'static str = "income_statement_test_database";

    /// Test the routines for the income statements db
    #[test]
    fn test_income_statements_db() {
        let db_user = env::var("DB_USER").expect("DB_USER not set");
        let port = env::var("DB_PORT").expect("DB_PORT not set");
        let password = env::var("DB_PASSWORD").expect("DB_PASSWORD not set");
        let host = env::var("DB_HOST").expect("DB_HOST not set");
        let db_name = env::var("DB_NAME").expect("DB_NAME not set");
        let db_url = format!("postgres://{db_user}:{password}@{host}:{port}/{db_name}");

        let mut db = Client::connect(&db_url, NoTls).unwrap();
        drop_database(&mut db);

        let current_date = chrono::Utc::now();
        let year = current_date.year();
        let month = current_date.month();
        let day = current_date.day();
        let date = if let Some(date) = chrono::NaiveDate::from_ymd_opt(year, month, day) {
            date
        } else {
            panic!("Could not determine date");
        };

        let income_statement = IncomeStatement {
            symbol: "SBKP.JO".to_string(),
            term: date,
            total_revenue: Some("1000.00".to_string()),
            income_from_associates_and_other_participating_interests: Some("1000.00".to_string()),
            special_income_charges: Some("1000.00".to_string()),
            other_non_operating_income_expenses: Some("1000.00".to_string()),
            pretax_income: Some("1000.00".to_string()),
            net_income_common_stockholders: Some("1000.00".to_string()),
            tax_provision: Some("1000.00".to_string()),
            net_income_from_continuing_operation_net_minority_interest: Some("1000.00".to_string()),
            diluted_ni_available_to_com_stockholders: Some("1000.00".to_string()),
            net_from_continuing_and_discontinued_operation: Some("1000.00".to_string()),
            normalized_income: Some("1000.00".to_string()),
            reconciled_depreciation: Some("1000.00".to_string()),
            total_unusual_items_excluding_goodwill: Some("1000.00".to_string()),
            total_unusual_items: Some("1000.00".to_string()),
            tax_rate_for_calcs: Some("1000.00".to_string()),
            tax_effect_of_unusual_items: Some("1000.00".to_string()),
            gross_profit: Some("1000.00".to_string()),
            cost_of_revenue: Some("1000.00".to_string()),
            operating_expense: Some("1000.00".to_string()),
            operating_income: Some("1000.00".to_string()),
            net_non_operating_interest_income_expense: Some("1000.00".to_string()),
            other_income_expense: Some("1000.00".to_string()),
            basic_eps: Some("1000.00".to_string()),
            diluted_eps: Some("1000.00".to_string()),
            basic_average_shares: Some("1000.00".to_string()),
            diluted_average_shares: Some("1000.00".to_string()),
            total_operating_income_as_reported: Some("1000.00".to_string()),
            total_expenses: Some("1000.00".to_string()),
            interest_income: Some("1000.00".to_string()),
            interest_expense: Some("1000.00".to_string()),
            net_interest_income: Some("1000.00".to_string()),
            rent_expense_supplemental: Some("1000.00".to_string()),
            ebit: Some("1000.00".to_string()),
            ebitda: Some("1000.00".to_string()),
            reconciled_cost_of_revenue: Some("1000.00".to_string()),
            normalized_ebitda: Some("1000.00".to_string()),
            average_dilution_earnings: Some("1000.00".to_string()),
            credit_losses_provision: Some("1000.00".to_string()),
            non_interest_expense: Some("1000.00".to_string()),
            interest_income_after_provision_for_loan_loss: Some("1000.00".to_string()),
            total_money_market_investments: Some("1000.00".to_string()),
            earnings_from_equity_interest_net_of_tax: Some("1000.00".to_string()),
            filed: date,
            version: 0,
        };

        let hash = &income_statement.hash();

        // Initialize the Income Statements database
        IncomeStatementsDB::init(&mut db, TABLE, &db_user).unwrap();

        // Save an Income Statement in the database
        IncomeStatementsDB::save(&mut db, TABLE, income_statement.clone()).unwrap();

        // Retrieved saved Income Statement from database, by using its hash
        let res = IncomeStatementsDB::read(&mut db, TABLE, &hash).unwrap();

        if let Some(res) = res {
            assert_eq!(res, income_statement.clone());

            // Read all income statements
            let res = IncomeStatementsDB::read_all(&mut db, TABLE).unwrap();

            assert_eq!(res.len(), 1);
            assert_eq!(res[0], income_statement.clone());

            // Read all income statements by symbol
            let res = IncomeStatementsDB::read_all_by_symbol(&mut db, TABLE, "SBKP.JO").unwrap();

            assert_eq!(res.len(), 1);
            assert_eq!(res[0], income_statement);
        } else {
            panic!("Error: Expected Income Statement Not Found!");
        }
    }

    fn drop_database(client: &mut Client) {
        let sql = format!("DROP TABLE IF EXISTS {TABLE};");

        client
            .batch_execute(&sql)
            .expect("Error: Could not drop database for income statements.");
    }
}
