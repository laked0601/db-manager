# db-manager
A functions in python that can convert .json &amp; .csv files into SQL databases

--- HOW TO USE ---
1. Collect a file that contains lines of valid json. For instance, here is the result of several API requests to the UK Companies House api:
	![image](https://user-images.githubusercontent.com/90655952/174459593-f0e8d9c7-ead2-43a0-8811-9d82cb27b07d.png)

2. Call "analyse_json()" on the above file's path. This will read through the file and collect a json tree that represents the most
   appropriate SQLite data types to use for creating the table.
   eg for the referenced data set this was produced:
   
		{
		  "_company_number": "TEXT",
		  "_has_been_liquidated": "INTEGER",
		  "_company_name": "TEXT",
		  "_type": "TEXT",
		  "_jurisdiction": "TEXT",
		  "_last_full_members_list_date": "TEXT",
		  "_undeliverable_registered_office_address": "INTEGER",
		  "_sic_codes": {
			"_sic_codes": "TEXT"
		  },
		  "_date_of_creation": "TEXT",
		  "_etag": "TEXT",
		  "_has_insolvency_history": "INTEGER",
		  "_has_charges": "INTEGER",
		  "_company_status": "TEXT",
		  "_previous_company_names": {
			"_ceased_on": "TEXT",
			"_name": "TEXT",
			"_effective_from": "TEXT"
		  },
		  "_registered_office_is_in_dispute": "INTEGER",
		  "_has_super_secure_pscs": "INTEGER",
		  "_can_file": "INTEGER",
		  "_registered_office_address_postal_code": "TEXT",
		  "_registered_office_address_region": "TEXT",
		  "_registered_office_address_address_line_1": "TEXT",
		  "_registered_office_address_locality": "TEXT",
		  "_registered_office_address_address_line_2": "TEXT",
		  "_accounts_next_due": "TEXT",
		  "_accounts_next_made_up_to": "TEXT",
		  "_accounts_overdue": "INTEGER",
		  "_confirmation_statement_next_due": "TEXT",
		  "_confirmation_statement_last_made_up_to": "TEXT",
		  "_confirmation_statement_next_made_up_to": "TEXT",
		  "_confirmation_statement_overdue": "INTEGER",
		  "_links_self": "TEXT",
		  "_links_filing_history": "TEXT",
		  "_links_officers": "TEXT",
		  "_links_charges": "TEXT",
		  "_links_persons_with_significant_control": "TEXT",
		  "_accounts_accounting_reference_date_month": "TEXT",
		  "_accounts_accounting_reference_date_day": "TEXT",
		  "_accounts_next_accounts_period_end_on": "TEXT",
		  "_accounts_next_accounts_overdue": "INTEGER",
		  "_accounts_next_accounts_due_on": "TEXT",
		  "_accounts_next_accounts_period_start_on": "TEXT",
		  "_accounts_last_accounts_made_up_to": "TEXT",
		  "_accounts_last_accounts_period_start_on": "TEXT",
		  "_accounts_last_accounts_type": "TEXT",
		  "_accounts_last_accounts_period_end_on": "TEXT",
		  "_registered_office_address_country": "TEXT",
		  "_date_of_cessation": "TEXT",
		  "_links_insolvency": "TEXT",
		  "_annual_return_overdue": "INTEGER",
		  "_annual_return_last_made_up_to": "TEXT",
		  "_links_registers": "TEXT",
		  "_links_exemptions": "TEXT",
		  "_links_persons_with_significant_control_statements": "TEXT",
		  "_registered_office_address_care_of": "TEXT",
		  "_annual_return_next_made_up_to": "TEXT",
		  "_annual_return_next_due": "TEXT",
		  "_company_status_detail": "TEXT",
		  "_registered_office_address_po_box": "TEXT",
		  "_status": "TEXT",
		  "_is_community_interest_company": "INTEGER",
		  "_subtype": "TEXT"
		}

3. Call "convert_json_to_sqlite_table()". This will convert the json tree in step 2 to a series of SQLlite queries that can be
   used to make the required tables.
   the output in sql looks like this:
   
![image](https://user-images.githubusercontent.com/90655952/174459650-09474939-5452-441a-8053-4a599cc4ad4e.png)

4. Create your tables with the above sqllite commands and add the json tree from step 2 to the table in the database called
   "json_structure". In order to read and insert the lines in the .json file, the json_structure must be findable in this
   table, else the code will not be able to know which keys are missing from a given line

5. Call "upload_json_to_sql()" with the file path to the .json file, the name of the main table that the data will go into
   (in this instance it is "company_data") and an SQLite connection object to your database
   

The above should work with any data set of your choice that consists of lines of parseable json.
