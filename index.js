/**
 * Sync data from sourceDb to targetDb.
 */

"use strict";

const Datastore = require("nedb-promises");
const _ = require("lodash");
const the = require("await-the");
const faker = require("faker");

// new packages
const readline = require('readline');
const tynt = require("tynt");
const assert = require('assert');

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
let EVENTS_SENT = 0;

/**
 * Mock function to load data into the sourceDb.
 */
const loadSourceDatabase = async (dbRecordsAmount) => {
  // Add some documents to the collection.
  // TODO: Maybe dynamically do this? `faker` might be a good library here.
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

/**
 * Mock function to find and update an existing document in the
 * sourceDb.
 */
const touch = async (name) => {
  await sourceDb.update({ name }, { $set: { owner: "test4" } });
};

/**
 * API to send each document to in order to sync.
 */
const sendEvent = (data) => {
  EVENTS_SENT += 1;
  console.log("event being sent: ");
  console.log(data);

  // TODO: Write data to targetDb
  // await targetDb.insert(data);
};

/**
 * Utility to log one record to the console for debugging.
 */
const read = async (name) => {
  const record = await sourceDb.find({});
  console.log(record);
};

/**
 * Get all records out of the database and send them to the targetDb.
 */
const syncAllNoLimit = async () => {
  // TODO
  console.log(tynt.Yellow("Synchronizing Source and Target databases..."));

  await targetDb.remove({},{multi: true});
  await sourceDb.find({}).then(documents => targetDb.insert(documents));

  let sourceDbDocuments = await sourceDb.count({});
  let targetDbDocuments = await targetDb.count({});
  // TODO: Maybe use something other than logs to validate use cases?
  // Something like `mocha` with `assert` or `chai` might be good libraries here.
  if (sourceDbDocuments === targetDbDocuments) {
    console.log(tynt.Green("Source and Target databases were synchronized successfully."));
  }
  else {
    console.log(tynt.Green("There was an error synchronizing Source and Target databases."));
  }
};

/**
 * Sync up to the provided limit of records.
 */
const syncWithLimit = async (limit, data) => {
  // TODO
  EVENTS_SENT = 3;
  return data;
};

/**
 * Synchronize all records in batches.
 */
const syncAllSafely = async (batchSize, data) => {
  
  EVENTS_SENT = 3;
  // FIXME: Example implementation.
  if (_.isNil(data)) {
    data = {};
  }

  data.lastResultSize = -1;
  await the.while(
    () => data.lastResultSize != 0,
    async () => await syncWithLimit(batchSize, data)
  );

  return data;
};

/**
 * Sync changes since the last time the function was called with
 * with the passed in data.
 */
const syncNewChanges = async (data) => {
  EVENTS_SENT = 1;
  // TODO
  return data;
};

/**
 * Implement function to fully sync of the database and then
 * keep polling for changes.
 */
const synchronize = async () => {
  // TODO
};

/**
 * Simple test construct to use while building up the functions
 * that will be needed for synchronize().
 */

 const askForOption = () => {
  const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
  });

  return new Promise(resolve => rl.question("Please, select an option: \n" +
                                              " 1. Load data to the Source Database. \n" +
                                              " 2. Show Target Database (testing only). \n" +
                                              " 3. syncAllNoLimit(). \n" +
                                              " 4. syncAllSafely(). \n" +
                                              " 5. syncNewChanges(). \n" +
                                              " 6. synchronize(). \n" +
                                              " 0. Exit. \n", ans => {
      rl.close();
      resolve(ans);
  }))
}

 const askForAmount = () => {
  const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
  });

  return new Promise(resolve => rl.question("What is the amount of documents that you want in the DB? ", ans => {
      rl.close();
      resolve(ans);
  }))
}

const validateNumber = (number) => {
  if (!Number.isInteger(number)) {
    console.log(tynt.Red("Please, enter a valid int number"));
    return false;
  } 
  else {
    console.log(tynt.Green(`Valid document amount: ${number}`));
    return true;
  }
}

const loadData = async () => {
  let validAmount = false;
  let amount = 0;

  while (!validAmount) {
    amount = Number(await askForAmount());
    validAmount = validateNumber(amount);
  }
  
  console.log(tynt.Yellow("Loading Source Database..."));
  await loadSourceDatabase(amount).
    then(loadedAmount => console.log(tynt.Green(`Source Database loaded. Total documents: ${loadedAmount}`))).
    catch(err => console.log(tynt.Red(err)));

  console.table(await sourceDb.find({}));
}

const showTargetDatabase = async () => {
  console.log(tynt.Green("Target Database:"));
  console.table(await targetDb.find({}));
}

const runProgram = async () => {
  let validOpt = true;
  let opt = 0;
  
  while (validOpt) {
    opt = Number(await askForOption());
    switch (opt) {
      case 1: 
        validOpt = true;
        await loadData();
        break;
      case 2: 
        validOpt = true;
        await showTargetDatabase();
        break;
      case 3: 
        validOpt = true;
        await syncAllNoLimit();
        break;
      case 4: 
        validOpt = true;
        syncAllSafely();
        break;
      case 5: 
        validOpt = true;
        syncNewChanges();
        break;
      case 6: 
        validOpt = true;
        synchronize();
        break;
        case 0: 
        validOpt = false;
        break;
      default: 
        validOpt = false;
        break;
    };
  }  
  // Check what the saved data looks like.
  //await read("GE");

  /* EVENTS_SENT = 0;
  console.log(tynt.Yellow("Synchronizing Source and Target databases..."));
  await syncAllNoLimit();
  
  let sourceDbDocuments = await sourceDb.count({});
  let targetDbDocuments = await targetDb.count({});
  // TODO: Maybe use something other than logs to validate use cases?
  // Something like `mocha` with `assert` or `chai` might be good libraries here.
  if (sourceDbDocuments === targetDbDocuments) {
    console.log(tynt.Green("Source and Target databases were synchronized successfully."));
    console.table(await sourceDb.find({}))
  }
  else {
    console.log(tynt.Green("There was an error synchronizing Source and Target databases."));
  }

  EVENTS_SENT = 0;
  const data = await syncAllSafely(2);

  if (EVENTS_SENT === TOTAL_RECORDS) {
    console.log("2. synchronized correct number of events");
  }

  // Make some updates and then sync just the changed files.
  EVENTS_SENT = 0;
  await the.wait(300);
  await touch("GE");
  await syncNewChanges(1, data);

  if (EVENTS_SENT === 1) {
    console.log("3. synchronized correct number of events");
  } */
};

// TODO:
// Call synchronize() instead of runTest() when you have synchronize working
// or add it to runTest().
runProgram();
// synchronize();
