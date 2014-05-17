SimpleDB
========
A simple key/value DB implementation in python / Learning exercise

####Limitations
 - only accepts `int` type for values
 - no concurrency
 - limited command set
 - lots of other stuffs
  

####things I tried to do
 - All commands persisted to a simple `.sdb` file
 - All commands issued within a transaction persisted to `pending.txt`
   - Simple write-ahead logging implementation
   - Prompts user to re-load any pending transactions on program crash
