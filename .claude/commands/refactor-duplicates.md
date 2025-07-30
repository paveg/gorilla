Run `similarity-generic .` to detect semantic code similarities. 

Execute this command, analyze the duplicate code patterns, and create a refactoring plan. Check `similarity-generic -h` for detailed options.

If you are on golang repository and need to parallel execution, you can use the following command. 
`fd -e go -x similarity-generic --language go`

