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
  EVENTS_SENT = 3;
  await sourceDb.find({}).then(documents => targetDb.insert(documents));
  
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

const runTest = async () => {
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

  
  // Check what the saved data looks like.
  //await read("GE");

  EVENTS_SENT = 0;
  console.log(tynt.Yellow("Synchronizing Source and Target databases..."));
  await syncAllNoLimit();
  let targetDbDocuments = await targetDb.find({});
  // TODO: Maybe use something other than logs to validate use cases?
  // Something like `mocha` with `assert` or `chai` might be good libraries here.
  if (EVENTS_SENT === TOTAL_RECORDS) {
    console.log("1. synchronized correct number of events");
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
  }
};

// TODO:
// Call synchronize() instead of runTest() when you have synchronize working
// or add it to runTest().
runTest();
// synchronize();
