/**
 * Sync data from sourceDb to targetDb.
 */

"use strict";

const Datastore = require("nedb-promises");
const _ = require("lodash");
const the = require("await-the");
const faker = require("faker");
let assert = require('assert');

// new packages
const readline = require('readline');
const tynt = require("tynt");

// The source database to sync updates from.
const sourceDb = new Datastore({
  inMemoryOnly: true,
  timestampData: true,
});

// The target database that sendEvents() will write too.
const targetDb = new Datastore({
  inMemoryOnly: true,
  timestampData: true,
});

let TOTAL_RECORDS = 0;

//used to save the last time the 2 databases were synchronized
let lastSync = new Date();

// used to save if the user wants to mantain both databases being synch automatically
let synchronizing = false;


// function that builds the menu
 const askForOption = () => {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

  return new Promise(resolve => rl.question("Please, select an option: \n" +
                                              " 1. Load data to the Source Database. \n" +
                                              " 2. Show Target Database (for testing only). \n" +
                                              " 3. Update a document with random data (for testing only). \n" +
                                              " 4. syncAllNoLimit(). \n" +
                                              " 5. syncAllSafely(). \n" +
                                              " 6. syncNewChanges(). \n" +
                                              " 7. Start/Stop synchronize(). \n" +
                                              " 0. Exit. \n", ans => {
      rl.close();
      resolve(ans);
  }))
}

/* 
  * function used for asking the user for a number/id
    *used in option 1 (asking for and validating the number of new docs to add in the current Source Database)
    *used in option 3 (asking for the id of the document to be updated, I know)
    *used in option 5 (asking for the batch size)
    *used in option 7 (asking for the interval of time that will be used between updates)
 */ 
const askForNumber = (question) => {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

  return new Promise(resolve => rl.question(question, ans => {
      rl.close();
      resolve(ans);
  }))
}
 
/* 
  * function that validates if the parameter is a number
  * preventing strings and decimal numbers 
*/
const validateNumber = (number) => {
  if (!Number.isInteger(number)) {
    console.log(tynt.Red("Please, enter a valid int number"));
    return false;
  } 
  else {
    console.log(tynt.Green(`Valid number: ${number}`));
    return true;
  }
}

/* 
  ** Option 1 **
    * Aks for the number of records to be added in the Source Database and validate the number
    * I used then() here to have the amount retrieved by loadSourceDatabase()
*/
const loadData = async () => {
  let validAmount = false;
  let amount = 0;

  while (!validAmount) {
    amount = Number(await askForNumber("What is the amount of documents that you want in the DB? "));
    validAmount = validateNumber(amount);
  }
  
  console.log(tynt.Yellow("Loading Source Database..."));
  await loadSourceDatabase(amount).
    then(loadedAmount => console.log(tynt.Green(`Source Database loaded. Total documents: ${loadedAmount}`))).
    catch(err => console.log(tynt.Red(err)));

  console.table(await sourceDb.find({}));
}

/**
 * function used to add records to the Source Database Once the amount is valid
 */
 const loadSourceDatabase = async (dbRecordsAmount) => {
  // Add some documents to the collection.
  for (var i=0; i < dbRecordsAmount; i++) {
    await sourceDb.insert({ 
      name: faker.company.companyName(), 
      owner: faker.name.firstName(), 
      amount: faker.datatype.number({ min: 10, max: 100})
    });
    TOTAL_RECORDS += 1;
  }
  return TOTAL_RECORDS;
};

/* 
  ** Option 2 ** 
    * Just showing what is on the Target Database
    * Useful for testing the app anytime
*/
const showTargetDatabase = async () => {
  console.log(tynt.Green("Target Database:"));
  console.table(await targetDb.find({}));
}

/* 
  ** Option 3 ** 
    * Function that tries to update the doc with the provided Id
    * and setting a new fake Owner's name in order to have that record updated
    * property 'returnUpdatedDocs' is setted 'true' to have the doc in case it exist 
    * in order to show it in the console
*/
const updateDocument = async () => {
    let id = await askForNumber("Enter an Id: ");
    const doc = await sourceDb.update({ _id: id }, { $set: { owner: faker.name.firstName() } }, {returnUpdatedDocs: true});
    if(doc) {
      console.table(doc);
      console.log(tynt.Green(`Document '${id}' updated successfully`));
    }
    else {
      console.log(tynt.Red(`Document '${id}' does not exist`));
    }
}

/* 
  ** Option 4 ** 
  * function that removes all the docs of the Target Database
  * and then just inserts all the docs found in the Source Database
*/
 const syncAllNoLimit = async () => {
  // TODO
  console.log(tynt.Yellow("Synchronizing Source and Target databases..."));

  await targetDb.remove({},{multi: true});
  await sourceDb.find({}).then(documents => targetDb.insert(documents));
  lastSync = new Date();
  console.log(tynt.Green("Source and Target databases were synchronized successfully."));
};

/* 
  ** Option 5 ** 
    * Function that asks for the batch size and validate it
    * then will clean the Target Database and start to keep inserting
    * new docs according to the batch size and skipping the records already inserted
*/
const syncAllSafely = async () => {
  let validAmount = false;
  let batchSize = 0;

  while (!validAmount) {
    batchSize = Number(await askForNumber("What is the batch size? "));
    validAmount = validateNumber(batchSize);
  }
  let skip = 0;
  let keepInserting = true;
  await targetDb.remove({},{multi: true});
  console.log(tynt.Yellow("Batching docs to the Target Database..."));

  while (keepInserting) {
    let docs = await sourceDb.find({}).skip(skip).limit(batchSize);
    if(docs.length === 0) {
      keepInserting = false;
      lastSync = new Date();
      console.log(tynt.Green("Source and Target databases were synchronized successfully."));
      
    }
    else {
      skip += batchSize;
      await targetDb.insert(docs);
    }
  }
};

/* 
  ** Option 6 ** 
  * Function that looks for the updated and new docs in the Source Database
    * updated docs: docs that the updated date is greater than the last sync date
    *               and created before that
    * new docs: docs created after the last sync
  * New docs are inserted in the target database as an array and
  * Updated docs will be updated by id
*/
const syncNewChanges = async () => {
  let updatedDocs = await sourceDb.find({
                                        $and:[
                                          {updatedAt: {$gt: lastSync}},
                                          {createdAt: {$lt: lastSync}},
                                        ]
                                      });
  let newDocs = await sourceDb.find({createdAt: {$gt: lastSync}});

  if (newDocs.length > 0) {
    await targetDb.insert(newDocs);
  }

  updatedDocs.forEach(doc => {
    targetDb.update({ _id: doc._id }, { $set: { 
                                  name: doc.name,
                                  owner: doc.owner,
                                  amount: doc.amount,
                                  updatedAt: new Date()
                                } 
    });
  })

  lastSync = new Date();
  console.log(tynt.Green(`Target Database updated successfully`));

};

/* 
  ** Option 7 ** 
  * Function that will start/stop the synchronization task
  * When starting, first runs syncAllNoLimit() to set a fresh sync
  * Then will ask the user for the interval of time for the sync
  * and run startSync() with that number
*/
const synchronize = async () => {
  let validNumber = false;
  let number = 0;
  synchronizing = !synchronizing;

  if (synchronizing) {
    while (!validNumber) {
      number = Number(await askForNumber("Enter update intervals (in secs) "));
      validNumber = validateNumber(number);
    }

    if(await targetDb.count({}) === 0) {
      await syncAllNoLimit();
    
    }
    console.log(tynt.Green(`Synchronization running...`));
    startSync(number);
  }
  else {
    console.log(tynt.Yellow(`Synchronization stopped.`));
  }
};

//function that calls syncNewChanges() until the user stop the sync
const startSync = async (interval) => {
  while (synchronizing) {
    await syncNewChanges();
    await the.wait(interval * 1000);
  }
}

const runProgram = async () => {
  let validOpt = true;
  let opt = 0;
  
  while (validOpt) {
    opt = Number(await askForOption());
    switch (opt) {
      case 1: 
        await loadData();
        break;
      case 2: 
        await showTargetDatabase();
        break;
      case 3: 
        await updateDocument();
        break;
      case 4: 
        await syncAllNoLimit();
        break;
      case 5: 
        await syncAllSafely();
        break;
      case 6: 
        await syncNewChanges();
        break;
      case 7: 
        await synchronize();
        break;
        case 0: 
        validOpt = false;
        break;
      default: 
        break;
    };
  }  
};


runProgram();

module.exports.validateNumber = (n) => {return validateNumber(n)};
module.exports.loadSourceDatabase = async (n) => {return await loadSourceDatabase(n)};
