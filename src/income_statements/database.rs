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
        symbol varchar(80),
        total_revenue varchar(100),
        income_from_associates_and_other_participating_interests varchar(100),
        special_income_charges varchar(100),
        other_non_operating_income_expenses varchar(100),
        pretax_income varchar(100),
        tax_provision varchar(100),
        net_income_from_continuing_operation_net_minority_interest varchar(100),
        diluted_ni_available_to_com_stockholders varchar(100),
        net_from_continuing_and_discontinued_operation varchar(100),
        normalized_income varchar(100),
        reconciled_depreciation varchar(100),
        total_unusual_items_excluding_goodwill varchar(100),
        total_unusual_items varchar(100),
        tax_rate_for_calcs varchar(100),
        tax_effect_of_unusual_items varchar(100),
        filed date,
        hash text UNIQUE,
        version smallint
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
        total_revenue,
        income_from_associates_and_other_participating_interests,
        special_income_charges,
        other_non_operating_income_expenses,
        pretax_income,
        tax_provision,
        net_income_from_continuing_operation_net_minority_interest,
        diluted_ni_available_to_com_stockholders,
        net_from_continuing_and_discontinued_operation,
        normalized_income,
        reconciled_depreciation,
        total_unusual_items_excluding_goodwill,
        total_unusual_items,
        tax_rate_for_calcs,
        tax_effect_of_unusual_items,
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
                    &income_statement.symbol,
                    &income_statement.total_revenue,
                    &income_statement.income_from_associates_and_other_participating_interests,
                    &income_statement.special_income_charges,
                    &income_statement.other_non_operating_income_expenses,
                    &income_statement.pretax_income,
                    &income_statement.tax_provision,
                    &income_statement.net_income_from_continuing_operation_net_minority_interest,
                    &income_statement.diluted_ni_available_to_com_stockholders,
                    &income_statement.net_from_continuing_and_discontinued_operation,
                    &income_statement.normalized_income,
                    &income_statement.reconciled_depreciation,
                    &income_statement.total_unusual_items_excluding_goodwill,
                    &income_statement.total_unusual_items,
                    &income_statement.tax_rate_for_calcs,
                    &income_statement.tax_effect_of_unusual_items,
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
                total_revenue: row[0].get("total_revenue"),
                income_from_associates_and_other_participating_interests: row[0]
                    .get("income_from_associates_and_other_participating_interests"),
                special_income_charges: row[0].get("special_income_charges"),
                other_non_operating_income_expenses: row[0]
                    .get("other_non_operating_income_expenses"),
                pretax_income: row[0].get("pretax_income"),
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
                total_revenue: r.get("total_revenue"),
                income_from_associates_and_other_participating_interests: r
                    .get("income_from_associates_and_other_participating_interests"),
                special_income_charges: r.get("special_income_charges"),
                other_non_operating_income_expenses: r.get("other_non_operating_income_expenses"),
                pretax_income: r.get("pretax_income"),
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
                total_revenue: r.get("total_revenue"),
                income_from_associates_and_other_participating_interests: r
                    .get("income_from_associates_and_other_participating_interests"),
                special_income_charges: r.get("special_income_charges"),
                other_non_operating_income_expenses: r.get("other_non_operating_income_expenses"),
                pretax_income: r.get("pretax_income"),
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
            total_revenue: "1000.00".to_string(),
            income_from_associates_and_other_participating_interests: "1000.00".to_string(),
            special_income_charges: "1000.00".to_string(),
            other_non_operating_income_expenses: "1000.00".to_string(),
            pretax_income: "1000.00".to_string(),
            tax_provision: "1000.00".to_string(),
            net_income_from_continuing_operation_net_minority_interest: "1000.00".to_string(),
            diluted_ni_available_to_com_stockholders: "1000.00".to_string(),
            net_from_continuing_and_discontinued_operation: "1000.00".to_string(),
            normalized_income: "1000.00".to_string(),
            reconciled_depreciation: "1000.00".to_string(),
            total_unusual_items_excluding_goodwill: "1000.00".to_string(),
            total_unusual_items: "1000.00".to_string(),
            tax_rate_for_calcs: "1000.00".to_string(),
            tax_effect_of_unusual_items: "1000.00".to_string(),
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
