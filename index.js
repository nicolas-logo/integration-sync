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
let lastSync = new Date();

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
  lastSync = new Date();
  console.log(tynt.Green("Source and Target databases were synchronized successfully."));

  // TODO: Maybe use something other than logs to validate use cases?
  // Something like `mocha` with `assert` or `chai` might be good libraries here.
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
const syncAllSafely = async () => {
  let validAmount = false;
  let batchSize = 0;

  while (!validAmount) {
    batchSize = Number(await askBatchSize());
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
  


/*   EVENTS_SENT = 3;
  // FIXME: Example implementation.
  if (_.isNil(data)) {
    data = {};
  }

  data.lastResultSize = -1;
  await the.while(
    () => data.lastResultSize != 0,
    async () => await syncWithLimit(batchSize, data)
  );

  return data; */
};

/**
 * Sync changes since the last time the function was called with
 * with the passed in data.
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
                                              " 2. Show Target Database (for testing only). \n" +
                                              " 3. Update a document with random data (for testing only). \n" +
                                              " 4. syncAllNoLimit(). \n" +
                                              " 5. syncAllSafely(). \n" +
                                              " 6. syncNewChanges(). \n" +
                                              " 7. synchronize(). \n" +
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

const askBatchSize = () => {
  const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
  });

  return new Promise(resolve => rl.question("What is the batch size? ", ans => {
      rl.close();
      resolve(ans);
  }))
}

const askDocId = () => {
  const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
  });

  return new Promise(resolve => rl.question("Enter an Id: ", ans => {
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
    console.log(tynt.Green(`Valid number: ${number}`));
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

const updateDocument = async () => {
    let id = await askDocId();
    const doc = await sourceDb.update({ _id: id }, { $set: { owner: faker.name.firstName() } }, {returnUpdatedDocs: true});
    if(doc) {
      console.table(doc);
      console.log(tynt.Green(`Document '${id}' updated successfully`));
    }
    else {
      console.log(tynt.Red(`Document '${id}' does not exist`));
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
