SimpleDB
========
A simple key/value DB implementation in python / Learning exercise with persistence and write-ahead-logging.
##Command Set / API
#### `SET <name> <value>`- Set the variable `name` to the value `value`. Neither variable names nor values can contain spaces.
![SET](http://i.imgur.com/CXD43nR.png)
####`GET <name>` - Get current value for a variable. Note that the context of the value for the variable is dependent on the transaction scope.
![GET](http://i.imgur.com/FznLvVP.png)
####`UNSET` - Delete the variable and it's value from the database completely.
![UNSET](http://i.imgur.com/FBzW2tz.png)
####`NUMEQUALTO <value>` - Get the current number of variables that are set to `value`
![NUMEQUALTO](http://i.imgur.com/19pRYZn.png)
####`BEGIN` - Start a transaction that will only be written to the database if issued the `COMMIT` command. Variables within a transaction will shadow their corresponding variables in the parent scope. In the case of an exit or crash, every command that is issued within a `BEGIN` clause will be persisted to the `pending.txt` file to be replayed if wanted. Successive transactions are indented to better display the depth.
![BEGIN1](http://i.imgur.com/RQqZm7X.png)  

![BEGIN2](http://i.imgur.com/uBrUmWj.png)  
####`COMMIT`-Save the currently set variables to database. Note that when issuing this command, it will commit the values of the **last** transaction currently in.
![COMMIT](http://i.imgur.com/dJJWlYR.png)
####`ROLLBACK` - Discard current transaction and rollback to previously defined values in the parent transaction.
![ROLLBACK1](http://i.imgur.com/diwXBCv.png)  

![ROLLBACK2](http://i.imgur.com/0hgZtfL.png)
####`Display` - Shows the currently committed values for all variables

##Somewhat Nifty - simple write-ahead-logging
![WRITEAHEAD1](http://i.imgur.com/3rJC1dQ.png)  

![WRITEAHEAD2](http://i.imgur.com/tv8Hwjx.png)  
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

========
