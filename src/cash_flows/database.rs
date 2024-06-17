use super::CashFlow;
use crate::error::Error;
use postgres::Client;

/// Database management for cash flow
pub struct CashFlowDB;
impl CashFlowDB {
    /// Initialize postgres cache
    pub fn init(client: &mut Client, table_name: &str, db_owner: &str) -> Result<(), Error> {
        let sql = format!(
            "
CREATE TABLE IF NOT EXISTS {table_name} (
	id serial NOT NULL,
        symbol varchar(80) NOT NULL,
        term date NOT NULL,
        cash_flows_from_used_in_operating_activities_direct varchar(100),
        operating_cash_flow varchar(100),
        investing_cash_flow varchar(100),
        financing_cash_flow varchar(100),
        end_cash_position varchar(100),
        capital_expenditure varchar(100),
        issuance_of_capital_stock varchar(100),
        issuance_of_debt varchar(100),
        repayment_of_debt varchar(100),
        repurchase_of_capital_stock varchar(100),
        free_cash_flow varchar(100),
        income_tax_paid_supplemental_data varchar(100),
        interest_paid_supplemental_data varchar(100),
        other_cash_adjustment_inside_change_in_cash varchar(100),
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

    /// Save cash sheet in database
    pub fn save(client: &mut Client, table_name: &str, cash_flow: CashFlow) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {table_name} (
        symbol,
        term,
        cash_flows_from_used_in_operating_activities_direct,
        operating_cash_flow,
        investing_cash_flow,
        financing_cash_flow,
        end_cash_position,
        capital_expenditure,
        issuance_of_capital_stock,
        issuance_of_debt,
        repayment_of_debt,
        repurchase_of_capital_stock,
        free_cash_flow,
        income_tax_paid_supplemental_data,
        interest_paid_supplemental_data,
        other_cash_adjustment_inside_change_in_cash,
        filed,
        hash,
        version
)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                         $12, $13, $14, $15, $16, $17, $18, $19);"
        );

        client
            .execute(
                &sql,
                &[
                    &cash_flow.symbol,
                    &cash_flow.term,
                    &cash_flow.cash_flows_from_used_in_operating_activities_direct,
                    &cash_flow.operating_cash_flow,
                    &cash_flow.investing_cash_flow,
                    &cash_flow.financing_cash_flow,
                    &cash_flow.end_cash_position,
                    &cash_flow.capital_expenditure,
                    &cash_flow.issuance_of_capital_stock,
                    &cash_flow.issuance_of_debt,
                    &cash_flow.repayment_of_debt,
                    &cash_flow.repurchase_of_capital_stock,
                    &cash_flow.free_cash_flow,
                    &cash_flow.income_tax_paid_supplemental_data,
                    &cash_flow.interest_paid_supplemental_data,
                    &cash_flow.other_cash_adjustment_inside_change_in_cash,
                    &cash_flow.filed,
                    &cash_flow.hash(),
                    &cash_flow.version,
                ],
            )
            .map_err(|_| Error::SQL)?;
        Ok(())
    }

    pub fn read(
        client: &mut Client,
        table_name: &str,
        hash: &str,
    ) -> Result<Option<CashFlow>, Error> {
        let sql = format!("SELECT * FROM {table_name} WHERE hash = $1");
        let row = client.query(&sql, &[&hash]).map_err(|_| Error::SQL)?;

        if row.is_empty() {
            Ok(None)
        } else {
            Ok(Some(CashFlow {
                symbol: row[0].get("symbol"),
                term: row[0].get("term"),
                cash_flows_from_used_in_operating_activities_direct: row[0]
                    .get("cash_flows_from_used_in_operating_activities_direct"),
                operating_cash_flow: row[0].get("operating_cash_flow"),
                investing_cash_flow: row[0].get("investing_cash_flow"),
                financing_cash_flow: row[0].get("financing_cash_flow"),
                end_cash_position: row[0].get("end_cash_position"),
                capital_expenditure: row[0].get("capital_expenditure"),
                issuance_of_capital_stock: row[0].get("issuance_of_capital_stock"),
                issuance_of_debt: row[0].get("issuance_of_debt"),
                repayment_of_debt: row[0].get("repayment_of_debt"),
                repurchase_of_capital_stock: row[0].get("repurchase_of_capital_stock"),
                free_cash_flow: row[0].get("free_cash_flow"),
                income_tax_paid_supplemental_data: row[0].get("income_tax_paid_supplemental_data"),
                interest_paid_supplemental_data: row[0].get("interest_paid_supplemental_data"),
                other_cash_adjustment_inside_change_in_cash: row[0]
                    .get("other_cash_adjustment_inside_change_in_cash"),
                version: row[0].get("version"),
                filed: row[0].get("filed"),
            }))
        }
    }

    pub fn read_all(client: &mut Client, table_name: &str) -> Result<Vec<CashFlow>, Error> {
        let sql = format!("SELECT * FROM {table_name}");
        let row = client.query(&sql, &[]).map_err(|_| Error::SQL)?;
        let mut cash_flow: Vec<CashFlow> = vec![];

        for r in &row {
            cash_flow.push(CashFlow {
                symbol: r.get("symbol"),
                term: r.get("term"),
                cash_flows_from_used_in_operating_activities_direct: r
                    .get("cash_flows_from_used_in_operating_activities_direct"),
                operating_cash_flow: r.get("operating_cash_flow"),
                investing_cash_flow: r.get("investing_cash_flow"),
                financing_cash_flow: r.get("financing_cash_flow"),
                end_cash_position: r.get("end_cash_position"),
                capital_expenditure: r.get("capital_expenditure"),
                issuance_of_capital_stock: r.get("issuance_of_capital_stock"),
                issuance_of_debt: r.get("issuance_of_debt"),
                repayment_of_debt: r.get("repayment_of_debt"),
                repurchase_of_capital_stock: r.get("repurchase_of_capital_stock"),
                free_cash_flow: r.get("free_cash_flow"),
                income_tax_paid_supplemental_data: r.get("income_tax_paid_supplemental_data"),
                interest_paid_supplemental_data: r.get("interest_paid_supplemental_data"),
                other_cash_adjustment_inside_change_in_cash: r
                    .get("other_cash_adjustment_inside_change_in_cash"),
                filed: r.get("filed"),
                version: r.get("version"),
            })
        }

        Ok(cash_flow)
    }

    pub fn read_all_by_symbol(
        client: &mut Client,
        table_name: &str,
        symbol: &str,
    ) -> Result<Vec<CashFlow>, Error> {
        let sql = format!("SELECT * FROM {table_name} WHERE symbol = $1");
        let row = client.query(&sql, &[&symbol]).map_err(|_| Error::SQL)?;
        let mut cash_flow: Vec<CashFlow> = vec![];

        for r in &row {
            cash_flow.push(CashFlow {
                symbol: r.get("symbol"),
                term: r.get("term"),
                cash_flows_from_used_in_operating_activities_direct: r
                    .get("cash_flows_from_used_in_operating_activities_direct"),
                operating_cash_flow: r.get("operating_cash_flow"),
                investing_cash_flow: r.get("investing_cash_flow"),
                financing_cash_flow: r.get("financing_cash_flow"),
                end_cash_position: r.get("end_cash_position"),
                capital_expenditure: r.get("capital_expenditure"),
                issuance_of_capital_stock: r.get("issuance_of_capital_stock"),
                issuance_of_debt: r.get("issuance_of_debt"),
                repayment_of_debt: r.get("repayment_of_debt"),
                repurchase_of_capital_stock: r.get("repurchase_of_capital_stock"),
                free_cash_flow: r.get("free_cash_flow"),
                income_tax_paid_supplemental_data: r.get("income_tax_paid_supplemental_data"),
                interest_paid_supplemental_data: r.get("interest_paid_supplemental_data"),
                other_cash_adjustment_inside_change_in_cash: r
                    .get("other_cash_adjustment_inside_change_in_cash"),
                filed: r.get("filed"),
                version: r.get("version"),
            })
        }

        Ok(cash_flow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use postgres::{Client, NoTls};
    use std::env;
    const TABLE: &'static str = "cash_flows_test_database";

    /// Test the routines for the cash flow db
    #[test]
    fn test_cash_flows_db() {
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

        let cash_flow = CashFlow {
            symbol: "SBKP.JO".to_string(),
            term: date,
            cash_flows_from_used_in_operating_activities_direct: Some("1000.00".to_string()),
            operating_cash_flow: Some("1000.00".to_string()),
            investing_cash_flow: Some("1000.00".to_string()),
            financing_cash_flow: Some("1000.00".to_string()),
            end_cash_position: Some("1000.00".to_string()),
            capital_expenditure: Some("1000.00".to_string()),
            issuance_of_capital_stock: Some("1000.00".to_string()),
            issuance_of_debt: Some("1000.00".to_string()),
            repayment_of_debt: Some("1000.00".to_string()),
            repurchase_of_capital_stock: Some("1000.00".to_string()),
            free_cash_flow: Some("1000.00".to_string()),
            income_tax_paid_supplemental_data: Some("1000.00".to_string()),
            interest_paid_supplemental_data: Some("1000.00".to_string()),
            other_cash_adjustment_inside_change_in_cash: Some("1000.00".to_string()),
            filed: date,
            version: 0,
        };

        let hash = &cash_flow.hash();

        // Initialize the Cash Sheets database
        CashFlowDB::init(&mut db, TABLE, &db_user).unwrap();

        // Save an Cash Flow in the database
        CashFlowDB::save(&mut db, TABLE, cash_flow.clone()).unwrap();

        // Retrieved saved Cash Flow from database, by using its hash
        let res = CashFlowDB::read(&mut db, TABLE, &hash).unwrap();

        if let Some(res) = res {
            assert_eq!(res, cash_flow.clone());

            // Read all cash flow
            let res = CashFlowDB::read_all(&mut db, TABLE).unwrap();

            assert_eq!(res.len(), 1);
            assert_eq!(res[0], cash_flow.clone());

            // Read all cash sheet by symbol
            let res = CashFlowDB::read_all_by_symbol(&mut db, TABLE, "SBKP.JO").unwrap();

            assert_eq!(res.len(), 1);
            assert_eq!(res[0], cash_flow);
        } else {
            panic!("Error: Expected Cash Flow Not Found!");
        }
    }

    fn drop_database(client: &mut Client) {
        let sql = format!("DROP TABLE IF EXISTS {TABLE};");

        client
            .batch_execute(&sql)
            .expect("Error: Could not drop database for cash flow.");
    }
}
