<div align="center">
  <img height="400" src="./assets/OtterSql.jpg"/>

  <h1>OtterSql</h1>

  <h3>An embeddable SQL executor</h3>
</div>

An in-memory database made for use in tests.

OtterSql implements a generic intermediate code (IC) with an **instruction set** for tabular data operations. This IC can be used to make in-memory mocks of larger databases such as PostgreSQL and MySQL. This IC is executed by the **OtterSql VM**. This project also provides a frontend that compiles a generic dialect of SQL to the IC.

The primary goal for this project is to facilitate developers in *testing* their SQL-backed applications.

Non-goals (for now): performance, multi-threading, thread safety, ACID compliance.

## FAQ

See [this blog post](#) introducing OtterSql.

## Use as a library

See [**the crate documentation**](https://docs.rs/otter-sql/latest).

## Features

### Currently implemented

- [x] `CREATE TABLE`/`CREATE SCHEMA`
- [x] `INSERT` values
- [x] Projection (`SELECT`ing specific columns)
- [x] Selection (`WHERE` clause of `SELECT`) including complex expressions
- [x] `LIMIT`
- [x] `ORDER BY`

### In-progress or in the near future

- [ ] `UPDATE`: execution
- [ ] Unions and Joins: execution
- [ ] Group by: execution
- [ ] Nested `SELECT`: codegen and execution
- [ ] Common table expressions (CTEs): codegen and execution
- [ ] Uphold table constraints: codegen and execution

### In the far future

- [ ] PostgreSQL codegen (frontend)
- [ ] MySQL codegen (frontend)

