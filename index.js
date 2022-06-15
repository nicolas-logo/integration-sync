/**
 * Sync data from sourceDb to targetDb.
 */

"use strict";

const Datastore = require("nedb-promises");
const _ = require("lodash");
const the = require("await-the");

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

let TOTAL_RECORDS;
let EVENTS_SENT = 0;

/**
 * Mock function to load data into the sourceDb.
 */
const load = async () => {
  // Add some documents to the collection.
  // TODO: Maybe dynamically do this? `faker` might be a good library here.
  await sourceDb.insert({ name: "GE", owner: "test", amount: 1000000 });
  await the.wait(300);
  await sourceDb.insert({ name: "Exxon", owner: "test2", amount: 5000000 });
  await the.wait(300);
  await sourceDb.insert({ name: "Google", owner: "test3", amount: 5000001 });

  TOTAL_RECORDS = 3;
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
  const record = await sourceDb.findOne({ name });
  console.log(record);
};

/**
 * Get all records out of the database and send them to the targetDb.
 */
const syncAllNoLimit = async () => {
  // TODO
};

/**
 * Sync up to the provided limit of records.
 */
const syncWithLimit = async (limit, data) => {
  // TODO
  return data;
};

/**
 * Synchronize all records in batches.
 */
const syncAllSafely = async (batchSize, data) => {
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
const runTest = async () => {
  await load();

  // Check what the saved data looks like.
  await read("GE");

  EVENTS_SENT = 0;
  await syncAllNoLimit();

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
