# Data Migration Script

This repository contains Golang scripts for migrating data between two databases. Currently only PostgreSQL and Oracle are supported.

## 📝 Description

The script helps with data migration between databases. It uses batching for select and insert queries to handle large amounts of data, and also includes progress tracking functionality.

## ⚙️ Prerequisites

- Golang 1.16 or later
- Oracle and/or PostgreSQL databases
- Oracle Instant Client (if using Oracle)

## 🚀 Usage

1. **Clone this repository.**

```bash
git clone https://github.com/primatime/database-migrator.git
```
2. **Navigate to the repository directory.**
```bash
cd database-migrator/src
```
3. **Build the Go scripts.**
```bash
go build -o migrator main.go  
```
4. **Run the migrator script to generate a tables structure into config.json file from source structure.**
Replace config.json with your configuration file path if it is different.
**You can skip this step if you defined table structure yourself.**

```bash
./migrator -config "config.json" -recreateConfig=true
```

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Edit the config.json file if necessary.

5. **Run the migrator.go script to migrate data.**

```bash
./migrator -config "config.json"
```

## 🔧 Configuration

Here is an example configuration file:

```json
{
  "source": {
    "dsn": "oracle_user/oracle_password@oracle_host:oracle_port/oracle_service_name",
    "schema": "public",
    "driver": "godror"
  },
  "target": {
    "dsn": "user=postgres_user password=postgres_password host=postgres_host port=postgres_port dbname=postgres_db sslmode=disable",
    "schema": "public",
    "driver": "postgres"
  },
  "batchSize": 1000,
  "tables": [
    {
      "name": "table1",
      "columns": ["column1", "column2", "column3"],
      "dependencies": []
    },
    {
      "name": "table2",
      "columns": ["column1", "column2", "column3"],
      "dependencies": ["table1"]
    }
  ]
}
```

## 👥 Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## 📜 License

MIT