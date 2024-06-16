use super::BalanceSheet;
use crate::error::Error;
use postgres::Client;

/// Database management for balance sheets
pub struct BalanceSheetsDB;
impl BalanceSheetsDB {
    /// Initialize postgres cache
    pub fn init(client: &mut Client, table_name: &str, db_owner: &str) -> Result<(), Error> {
        let sql = format!(
            "
CREATE TABLE IF NOT EXISTS {table_name} (
	id serial NOT NULL,
        symbol varchar(80) NOT NULL,
        term date NOT NULL,
        total_assets varchar(100),
        total_liabilities_net_minority_interest varchar(100),
        total_equity_gross_minority_interest varchar(100),
        total_capitalization varchar(100),
        preferred_stock_equity varchar(100),
        common_stock_equity varchar(100),
        net_tangible_assets varchar(100),
        invested_capital varchar(100),
        tangible_book_value varchar(100),
        total_debt varchar(100),
        net_debt varchar(100),
        share_issued varchar(100),
        ordinary_shares_number varchar(100),
        preferred_shares_number varchar(100),
        treasury_shares_number varchar(100),
        working_capital varchar(100),
        capital_lease_obligations varchar(100),
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

    /// Save balance sheet in database
    pub fn save(
        client: &mut Client,
        table_name: &str,
        balance_sheet: BalanceSheet,
    ) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {table_name} (
        symbol,
        term,
        total_assets,
        total_liabilities_net_minority_interest,
        total_equity_gross_minority_interest,
        total_capitalization,
        preferred_stock_equity,
        common_stock_equity,
        net_tangible_assets,
        invested_capital,
        tangible_book_value,
        total_debt,
        net_debt,
        share_issued,
        ordinary_shares_number,
        preferred_shares_number,
        treasury_shares_number,
        working_capital,
        capital_lease_obligations,
        filed,
        hash,
        version
)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                         $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22);"
        );

        client
            .execute(
                &sql,
                &[
                    &balance_sheet.symbol,
                    &balance_sheet.term,
                    &balance_sheet.total_assets,
                    &balance_sheet.total_liabilities_net_minority_interest,
                    &balance_sheet.total_equity_gross_minority_interest,
                    &balance_sheet.total_capitalization,
                    &balance_sheet.preferred_stock_equity,
                    &balance_sheet.common_stock_equity,
                    &balance_sheet.net_tangible_assets,
                    &balance_sheet.invested_capital,
                    &balance_sheet.tangible_book_value,
                    &balance_sheet.total_debt,
                    &balance_sheet.net_debt,
                    &balance_sheet.share_issued,
                    &balance_sheet.ordinary_shares_number,
                    &balance_sheet.preferred_shares_number,
                    &balance_sheet.treasury_shares_number,
                    &balance_sheet.working_capital,
                    &balance_sheet.capital_lease_obligations,
                    &balance_sheet.filed,
                    &balance_sheet.hash(),
                    &balance_sheet.version,
                ],
            )
            .map_err(|_| Error::SQL)?;
        Ok(())
    }

    pub fn read(
        client: &mut Client,
        table_name: &str,
        hash: &str,
    ) -> Result<Option<BalanceSheet>, Error> {
        let sql = format!("SELECT * FROM {table_name} WHERE hash = $1");
        let row = client.query(&sql, &[&hash]).map_err(|_| Error::SQL)?;

        if row.is_empty() {
            Ok(None)
        } else {
            Ok(Some(BalanceSheet {
                symbol: row[0].get("symbol"),
                term: row[0].get("term"),
                total_assets: row[0].get("total_assets"),
                total_liabilities_net_minority_interest: row[0]
                    .get("total_liabilities_net_minority_interest"),
                total_equity_gross_minority_interest: row[0]
                    .get("total_equity_gross_minority_interest"),
                total_capitalization: row[0].get("total_capitalization"),
                preferred_stock_equity: row[0].get("preferred_stock_equity"),
                common_stock_equity: row[0].get("common_stock_equity"),
                net_tangible_assets: row[0].get("net_tangible_assets"),
                invested_capital: row[0].get("invested_capital"),
                tangible_book_value: row[0].get("tangible_book_value"),
                total_debt: row[0].get("total_debt"),
                net_debt: row[0].get("net_debt"),
                share_issued: row[0].get("share_issued"),
                ordinary_shares_number: row[0].get("ordinary_shares_number"),
                preferred_shares_number: row[0].get("preferred_shares_number"),
                treasury_shares_number: row[0].get("treasury_shares_number"),
                working_capital: row[0].get("working_capital"),
                capital_lease_obligations: row[0].get("capital_lease_obligations"),
                version: row[0].get("version"),
                filed: row[0].get("filed"),
            }))
        }
    }

    pub fn read_all(client: &mut Client, table_name: &str) -> Result<Vec<BalanceSheet>, Error> {
        let sql = format!("SELECT * FROM {table_name}");
        let row = client.query(&sql, &[]).map_err(|_| Error::SQL)?;
        let mut balance_sheet: Vec<BalanceSheet> = vec![];

        for r in &row {
            balance_sheet.push(BalanceSheet {
                symbol: r.get("symbol"),
                term: r.get("term"),
                total_assets: r.get("total_assets"),
                total_liabilities_net_minority_interest: r
                    .get("total_liabilities_net_minority_interest"),
                total_equity_gross_minority_interest: r.get("total_equity_gross_minority_interest"),
                total_capitalization: r.get("total_capitalization"),
                preferred_stock_equity: r.get("preferred_stock_equity"),
                common_stock_equity: r.get("common_stock_equity"),
                net_tangible_assets: r.get("net_tangible_assets"),
                invested_capital: r.get("invested_capital"),
                tangible_book_value: r.get("tangible_book_value"),
                total_debt: r.get("total_debt"),
                net_debt: r.get("net_debt"),
                share_issued: r.get("share_issued"),
                ordinary_shares_number: r.get("ordinary_shares_number"),
                preferred_shares_number: r.get("preferred_shares_number"),
                treasury_shares_number: r.get("treasury_shares_number"),
                working_capital: r.get("working_capital"),
                capital_lease_obligations: r.get("capital_lease_obligations"),
                filed: r.get("filed"),
                version: r.get("version"),
            })
        }

        Ok(balance_sheet)
    }

    pub fn read_all_by_symbol(
        client: &mut Client,
        table_name: &str,
        symbol: &str,
    ) -> Result<Vec<BalanceSheet>, Error> {
        let sql = format!("SELECT * FROM {table_name} WHERE symbol = $1");
        let row = client.query(&sql, &[&symbol]).map_err(|_| Error::SQL)?;
        let mut balance_sheet: Vec<BalanceSheet> = vec![];

        for r in &row {
            balance_sheet.push(BalanceSheet {
                symbol: r.get("symbol"),
                term: r.get("term"),
                total_assets: r.get("total_assets"),
                total_liabilities_net_minority_interest: r
                    .get("total_liabilities_net_minority_interest"),
                total_equity_gross_minority_interest: r.get("total_equity_gross_minority_interest"),
                total_capitalization: r.get("total_capitalization"),
                preferred_stock_equity: r.get("preferred_stock_equity"),
                common_stock_equity: r.get("common_stock_equity"),
                net_tangible_assets: r.get("net_tangible_assets"),
                invested_capital: r.get("invested_capital"),
                tangible_book_value: r.get("tangible_book_value"),
                total_debt: r.get("total_debt"),
                net_debt: r.get("net_debt"),
                share_issued: r.get("share_issued"),
                ordinary_shares_number: r.get("ordinary_shares_number"),
                preferred_shares_number: r.get("preferred_shares_number"),
                treasury_shares_number: r.get("treasury_shares_number"),
                working_capital: r.get("working_capital"),
                capital_lease_obligations: r.get("capital_lease_obligations"),
                filed: r.get("filed"),
                version: r.get("version"),
            })
        }

        Ok(balance_sheet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use postgres::{Client, NoTls};
    use std::env;
    const TABLE: &'static str = "balance_sheet_test_database";

    /// Test the routines for the balance sheets db
    #[test]
    fn test_balance_sheets_db() {
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

        let balance_sheet = BalanceSheet {
            symbol: "SBKP.JO".to_string(),
            term: date,
            total_assets: Some("1000.00".to_string()),
            total_liabilities_net_minority_interest: Some("1000.00".to_string()),
            total_equity_gross_minority_interest: Some("1000.00".to_string()),
            total_capitalization: Some("1000.00".to_string()),
            preferred_stock_equity: Some("1000.00".to_string()),
            common_stock_equity: Some("1000.00".to_string()),
            net_tangible_assets: Some("1000.00".to_string()),
            invested_capital: Some("1000.00".to_string()),
            tangible_book_value: Some("1000.00".to_string()),
            total_debt: Some("1000.00".to_string()),
            net_debt: Some("1000.00".to_string()),
            share_issued: Some("1000.00".to_string()),
            ordinary_shares_number: Some("1000.00".to_string()),
            preferred_shares_number: Some("1000.00".to_string()),
            treasury_shares_number: Some("1000.00".to_string()),
            working_capital: Some("1000.00".to_string()),
            capital_lease_obligations: Some("1000.00".to_string()),
            filed: date,
            version: 0,
        };

        let hash = &balance_sheet.hash();

        // Initialize the Balance Sheets database
        BalanceSheetsDB::init(&mut db, TABLE, &db_user).unwrap();

        // Save an Balance Sheet in the database
        BalanceSheetsDB::save(&mut db, TABLE, balance_sheet.clone()).unwrap();

        // Retrieved saved Balance Sheet from database, by using its hash
        let res = BalanceSheetsDB::read(&mut db, TABLE, &hash).unwrap();

        if let Some(res) = res {
            assert_eq!(res, balance_sheet.clone());

            // Read all balance sheets
            let res = BalanceSheetsDB::read_all(&mut db, TABLE).unwrap();

            assert_eq!(res.len(), 1);
            assert_eq!(res[0], balance_sheet.clone());

            // Read all balance sheet by symbol
            let res = BalanceSheetsDB::read_all_by_symbol(&mut db, TABLE, "SBKP.JO").unwrap();

            assert_eq!(res.len(), 1);
            assert_eq!(res[0], balance_sheet);
        } else {
            panic!("Error: Expected Balance Sheet Not Found!");
        }
    }

    fn drop_database(client: &mut Client) {
        let sql = format!("DROP TABLE IF EXISTS {TABLE};");

        client
            .batch_execute(&sql)
            .expect("Error: Could not drop database for balance sheets.");
    }
}
