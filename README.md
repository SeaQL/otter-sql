# SqlAssembly - An embeddable SQL Interpreter

This project consists of three major parts: a high-level instruction set (aka bytecode) that supports SQL data manipulation and query, an interpreter that executes the bytecode and a frontend that transforms a generic SQL AST into the bytecode.

The primary goal for this project is to facilitate developers in testing their SQL-backed applications, but in the long term when this technology matures we hope this can become an embedded SQL engine for client-side applications.
