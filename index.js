const AWS = require('aws-sdk');
const dynamodb = new AWS.DynamoDB.DocumentClient();
let projectTable = "BTProject";
let projectLineTable = "BTProjectLines";
let projectsTable = "BTProjects";

const COMPANY_GSI_NAME = "company";
const COMPANY_GSI_PARTITION_KEY = "company";
const COMPANY_GSI_SORT_KEY = "sort";
const COMPANY_GSI_PARTITION_KEY_PATH = '/:' + COMPANY_GSI_PARTITION_KEY;
const COMPANY_GSI_SORT_KEY_PATH = '/:' + COMPANY_GSI_SORT_KEY;

//Rename tables according to current environment

if (process.env.ENV && process.env.ENV !== "NONE") {
    projectTable = projectTable + '-' + process.env.ENV;
    projectLineTable = projectLineTable + '-' + process.env.ENV;
    projectsTable = projectsTable + '-' + process.env.ENV;
}

function splitInBatches(lines, operation) {
    const batches = [];
    let currentBatch = [];
    //Need to create batches of 25 items
    for (let i = 0, n = lines.length; i < n; ++i) {
        if (currentBatch.length > 0 && i % 25 === 0) {
            batches.push(currentBatch);
            currentBatch = [];
        }
        const l = lines[i];
        currentBatch.push({
            [operation]: {
                Item: l
            }
        });
    }
    //push last batch
    if (currentBatch.length > 0) {
        batches.push(currentBatch);
    }
    return batches;
}
function splitInInsertBatches(lines) {
    return splitInBatches(lines, 'PutRequest');
}

function writeAllBatches(tableName, batches) {

    const promises = [];
    for (let i = 0, n = batches.length; i < n; i++) {
        const batch = batches[i];
        if (batch.length > 0) {
            const batchRequest = {
                RequestItems: {},
                ReturnConsumedCapacity: "TOTAL",
            };
            batchRequest.RequestItems[tableName] = batch;
            promises.push(dynamodb.batchWrite(batchRequest).promise());
        }
    }
    return promises;
}
const batchWriteCallback = function (err, batchResult) {
    console.warn('BATCH Result', batchResult);
    if (err) {
        console.error(err);
    }
    else if (Object.keys(batchResult.UnprocessedItems).length > 0) {
        console.warn('Retry batchWrite Operation', batchResult.UnprocessedItems);
        const params = {};
        params.RequestItems = batchResult.UnprocessedItems;
        dynamodb.batchWriteItem(params, batchWriteCallback);
    }
};



exports.splitInInsertBatches = splitInInsertBatches;
exports.writeAllBatches = writeAllBatches;

exports.dynamodb = dynamodb;
exports.projectTable = projectTable;
exports.projectLineTable = projectLineTable;
exports.projectsTable = projectsTable

exports.COMPANY_GSI_NAME = COMPANY_GSI_NAME;
exports.COMPANY_GSI_PARTITION_KEY = COMPANY_GSI_PARTITION_KEY;
exports.COMPANY_GSI_SORT_KEY = COMPANY_GSI_SORT_KEY;