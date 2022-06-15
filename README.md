### Coding Challenge Guidelines

This exercise has you implement a synchronize() method that will copy all records from the sourceDb into the targetDb() then start polling for changes. Places where you need to add code have been
mostly marked and the goal is to get the runTest() to complete successfully.

#### Requirements

Try to solve the following pieces for a production system. Note, doing one well is better than doing all poorly. If you are unable to complete certain requirements, please comment your approach to the solution. Comments on the "why" you chose to implement the code the way you did is highly preferred.

1. syncAllNoLimit(): Make sure what is in the source database is in our target database for all data in one sync. Think of a naive solution.
2. syncAllSafely(): Make sure what is in the source database is in our target database for all data in batches during multiple syncs. Think of a pagination solution.
3. syncNewChanges(): Make sure updates in the source database are in our target database without syncing all data for all time. Think of a delta change solution using operators.
4. synchronize(): Create polling for the sync. If haven't synced before, full sync all records from the source database. Otherwise, a cadence where we check the source database for updates to sync.

### Evaluation Criteria

#### Assumptions

Assume that you only have read-only access to the sourceDb and write-only access to the targetDb. Of course, for tests, you can go outside these assumptions to properly test; however, for the general logic, assume this level of access.

#### Code

Feel free to use any libraries that you are comfortable with and
change the parameters and returns to solve for the requirements.

#### Help

Reach out to rspoone@mediafly.com for clarity and general questions.

### Useful Links

You will need to reference the following API documentation:

* Required: https://www.npmjs.com/package/nedb
* Recommended: https://lodash.com/docs/4.17.15
* Recommended: https://www.npmjs.com/package/await-the#api-reference
