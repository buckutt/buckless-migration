const rethink = require('rethinkdb');
const mysql   = require('mysql');
const Promise = require('bluebird');

/**
 * Config
 */

const config = {
    db: 'bucklessServer_example'
};

/**
 * Connection objects
 */

const sqlCon = Promise.promisifyAll(mysql.createConnection({
    host    : 'localhost',
    user    : 'root',
    password: '',
    database: 'buckutt'
}));

let nosqlCon = null;

/**
 * Cache old sql primary keys <=> noSQL documents guid for association migration
 */
const groups      = {};
const mols        = {};
const users       = {};

const groupByName = {};

/**
 * Minimal seeds
 */

const points = [
    {
        name     : 'Foyer',
        createdAt: new Date(),
        editedAt : new Date(),
        isRemoved: false
    }
];

const events = [
    {
        name  : 'BDE UTT',
        config: {
            minReload    : 100,
            maxPerAccount: 10000
        },
        createdAt: new Date(),
        editedAt : new Date(),
        isRemoved: false
    }
];

let periods = [
    {
        name     : 'Éternité',
        start    : new Date(2010, 1, 1),
        end      : new Date(2050, 1, 1),
        createdAt: new Date(),
        editedAt : new Date(),
        isRemoved: false
    },
    {
        name     : 'A16',
        start    : new Date(2016, 9, 5),
        end      : new Date(2017, 3, 6),
        createdAt: new Date(),
        editedAt : new Date(),
        isRemoved: false
    },
    {
        name     : 'P17',
        start    : new Date(2017, 2, 20),
        end      : new Date(2017, 9, 18),
        createdAt: new Date(),
        editedAt : new Date(),
        isRemoved: false
    }
];

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

function resetDataBase() {
    return rethink.tableList().run(nosqlCon)
        .then((tableList) => {
            const deletePromises = [];

            tableList.forEach((table) => {
                deletePromises.push(rethink.table(table).delete().run(nosqlCon));
            });

            return Promise.all(deletePromises);
        })
        .then(() => {
            console.log('[OK] NoSQL data reset');
        });
}

function addUsers() {
    // Fetch and add all users
    const userPromises = [];
    return sqlCon.queryAsync('SELECT * FROM Users')
        .then(rows => {
            rows.forEach((user) => {
                // Add user
                // Filter `null`
                const credit   = (user.credit) ? user.credit : 0;
                const pin      = (user.pin) ? user.pin : 'togen';
                const password = (user.password) ? user.password : 'togen';
                const nickname = (user.nickname) ? user.nickname : '';

                userPromises.push(
                    rethink
                        .table('User')
                        .insert({
                            firstname  : user.firstname,
                            lastname   : user.lastname,
                            nickname,
                            pin,
                            password,
                            mail       : user.mail,
                            credit,
                            isTemporary: !!user.isTemporary,
                            isRemoved  : !!user.isRemoved,
                            createdAt  : new Date(),
                            editedAt   : new Date()
                        })
                        .run(nosqlCon)
                        .then(doc => {
                            users[user.id] = doc.generated_keys[0];
                        })
                        .then(() => {
                            // Filter `0`
                            if (!user.credit) {
                                return;
                            }

                            rethink
                                .table('MeanOfLogin')
                                .insert({
                                    type     : 'etuMail',
                                    data     : user.mail,
                                    blocked  : false,
                                    isRemoved: false,
                                    User_id  : users[user.id],
                                    createdAt: new Date(),
                                    editedAt : new Date()
                                })
                                .run(nosqlCon)
                                .catch(() => console.log(`[ERROR] MeanOfLogin, UserId: ${user.id},
                                    Type: mail`));

                            // Add initial reload
                            return rethink
                                .table('Reload')
                                .insert({
                                    User_id  : users[user.id],
                                    credit,
                                    trace    : 'Initial reload',
                                    isRemoved: false,
                                    createdAt: new Date(),
                                    editedAt : new Date()
                                })
                                .run(nosqlCon);
                        })
                );
            });
        })
        .then(() => Promise.all(userPromises))
        .then(() => console.log('[OK] Users added'));
}

function addGroups() {
    // Fetch and add all groups
    const groupPromises = [];
    return sqlCon.queryAsync('SELECT * FROM Groups')
        .then(rows => {
            rows.forEach(group => {
                // Add group
                groupPromises.push(
                    rethink
                        .table('Group')
                        .insert({
                            name     : group.name,
                            isOpen   : !!group.isOpen,
                            isPublic : !!group.isPublic,
                            isRemoved: !!group.isRemoved,
                            createdAt: new Date(),
                            editedAt : new Date()
                        })
                        .run(nosqlCon)
                        .then(doc => {
                            // Cache sql ID <=> document guid
                            groups[group.id]        = doc.generated_keys[0];
                            groupByName[group.name] = doc.generated_keys[0];
                        })
                );
            });
        })
        .then(() => Promise.all(groupPromises))
        .then(() => console.log('[OK] Groups added'));
}

function fetchMols() {
    // Fetch all MoL
    return sqlCon.queryAsync('SELECT * FROM MeanOfLogins')
        .then(rows => {
            rows.forEach(mol => {
                // Cache sql ID <=> sql name
                mols[mol.id] = mol.name;
            });
        })
        .then(() => console.log('[INFO] MoL fetched'));
}

function addMols() {
    // Fetch and add all UserMoL
    const usersMolsPromises = [];

    return sqlCon.queryAsync('SELECT * FROM MeanOfLoginsUsers')
        .then(rows => {
            rows.forEach(molUser => {
                // Add MolUser
                if (!users[molUser.UserId] || !mols[molUser.MeanOfLoginId]) {
                    return;
                }

                const type = (mols[molUser.MeanOfLoginId] === 'carte_etu') ? 'etuId' : mols[molUser.MeanOfLoginId];

                usersMolsPromises.push(
                    rethink
                        .table('MeanOfLogin')
                        .insert({
                            type,
                            data     : molUser.data,
                            blocked  : false,
                            isRemoved: !!molUser.isRemoved,
                            User_id  : users[molUser.UserId],
                            createdAt: new Date(),
                            editedAt : new Date()
                        })
                        .run(nosqlCon)
                        .catch(() => console.log(`[ERROR] MolUser, UserId: ${molUser.UserId},
                            GroupId: ${molUser.MeanOfLoginId}`))
                );
            });
        })
        .then(() => Promise.all(usersMolsPromises))
        .then(() => console.log('[OK] MeanOfLogin added'));
}

function addGroupPeriod() {
    const translatePeriods = [
        null,
        periods[0],
        null,
        null,
        periods[1],
        periods[2]
    ];

    // Fetch and add all UserGroup
    const usersGroupsPromises = [];
    return sqlCon.queryAsync('SELECT * FROM UsersGroups WHERE PeriodId IN(1, 4, 5)')
        .then(rows => {
            rows.forEach(userGroup => {
                // Add MolUser
                if (!groups[userGroup.GroupId] || !users[userGroup.UserId]) {
                    return;
                }

                usersGroupsPromises.push(
                    rethink
                        .table('GroupPeriod')
                        .insert({ Group_id: groups[userGroup.GroupId], Period_id: translatePeriods[userGroup.PeriodId].id })
                        .run(nosqlCon)
                        .then(doc => {
                            return rethink
                                .table('GroupPeriod_User')
                                .insert({ GroupPeriod_id: doc.generated_keys[0], User_id: users[userGroup.UserId] })
                                .run(nosqlCon)
                                .catch(() => console.log(`[ERROR] GroupPeriod_User, GroupPeriodId: ${doc.generated_keys[0]},
                                    UserId: ${userGroup.UserId}`))
                        })
                        .catch(() => console.log(`[ERROR] GroupPeriod, PeriodId: ${userGroup.PeriodId},
                            GroupId: ${userGroup.GroupId}`))
                );
            });
        })
        .then(() => Promise.all(usersGroupsPromises))
        .then(() => console.log('[OK] GroupPeriod added'));
}

/**
 * Seed functions
 */

function setKeys(insts, keys) {
    for (let i = 0; i < insts.length; i++) {
        insts[i].id = keys[i];
    }
}

function seedData() {
    return rethink.table('Event').insert(events).run(nosqlCon)
        .then(cursor => setKeys(events, cursor.generated_keys))
        .then(() => {
            periods = periods.map(period => {
                period.Event_id = events[0].id;

                return period;
            });

            return rethink
                .table('Period').insert(periods).run(nosqlCon)
                .then(cursor => {
                    setKeys(periods, cursor.generated_keys);
                })
            }
        )
        .then(() =>
            rethink
                .table('Point').insert(points).run(nosqlCon)
                .then(cursor => {
                    setKeys(points, cursor.generated_keys);
                })
        )
        .then(() => console.log('[OK] Data seeds'));
}

/**
 * Entry point
 */

connectToMaria()
    .then(connectToRethink)
    .then(addUsers)
    .then(addGroups)
    .then(fetchMols)
    .then(addMols)
    .then(seedData)
    .then(addGroupPeriod)
    .then(closeSqlCon)
    .then(closeNosqlCon)
    .catch(error => {
        console.log(`[ERR] ${error.stack}`);
        return process.exit(1);
    });
