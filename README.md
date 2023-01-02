# OtterSql - An embeddable SQL Interpreter

This project consists of three major parts: a high-level instruction set (aka bytecode) that supports SQL data manipulation and query, an interpreter that executes the bytecode and a frontend that transforms a generic SQL AST into the bytecode.

The primary goal for this project is to facilitate developers in testing their SQL-backed applications, but in the long term when this technology matures we hope this can become a pure Rust embedded SQL engine for client-side applications.

Right now, it is a pure in-memory database. ACID and execution performance is not the current goal of this project, but we will address them in the future!
