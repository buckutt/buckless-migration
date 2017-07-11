const rethink = require('rethinkdb');
const mysql   = require('mysql');
const Promise = require('bluebird');

/**
 * Config
 */

const config = {
    db: 'buckuttMigration'
};

/**
 * Connection objects
 */

const sqlCon = Promise.promisifyAll(mysql.createConnection({
    host    : 'localhost',
    user    : 'root',
    password: '',
    database: 'buckuttMigration'
}));

let nosqlCon = null;

/**
 * Migration functions
 */

function connectToMaria() {
    return sqlCon.connectAsync()
        .then(() => {
            console.log('[INFO] Connected to mariadb');
        });
}

function connectToRethink() {
    return rethink.connect({ db: config.db })
        .then((con) => {
            console.log('[INFO] Connected to rethinkdb');
            nosqlCon = con;
        });
}

function closeSqlCon() {
    return sqlCon.endAsync()
        .then(() => {
            console.log('[INFO] Mariadb connection closed');
        });
}

function closeNosqlCon() {
    return nosqlCon.close()
        .then(() => {
            console.log('[INFO] Rethinkdb connection closed');
        });
}

function updateUserCredit() {
    // Fetch and add all users
    const userPromises = [];
    return sqlCon.queryAsync('SELECT * FROM Users')
        .then(rows => {
            rows.forEach((user) => {
                const credit = (user.credit) ? user.credit : 0;

                userPromises.push(
                    rethink
                        .table('User')
                        .filter({
                            firstname  : user.firstname,
                            lastname   : user.lastname,
                            mail       : user.mail
                        })
                        .filter(rethink.row('credit').ne(credit))
                        .update({
                            credit: credit
                        }, { returnChanges: true })
                        .run(nosqlCon)
                        .then((res) => {
                            if (res.replaced === 0) {
                                return
                            }

                            console.log(res.changes[0].new_val.firstname, res.changes[0].new_val.lastname, 'has different credit');
                            console.log(res.changes[0].old_val.credit, res.changes[0].new_val.credit);

                            const userGuid = res.changes[0].new_val.id;

                            return rethink
                                .table('Reload')
                                .getAll(userGuid, { index: 'Seller_id' })
                                .filter({
                                    Buyer_id : userGuid,
                                    type     : 'gift',
                                    trace    : 'Transfert de l\'ancien solde BuckUTT'
                                })
                                .update({
                                    credit
                                })
                                .run(nosqlCon);
                        })
                );
            });
        })
        .then(() => Promise.all(userPromises))
        .then(() => console.log('[OK] Users updated '));
}


/**
 * Entry point
 */

connectToMaria()
    .then(connectToRethink)
    .then(updateUserCredit)
    .then(closeSqlCon)
    .then(closeNosqlCon)
    .catch(error => {
        console.error(error);
        console.log(`[ERR] ${error.stack}`);
        return process.exit(1);
    });
