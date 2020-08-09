const Database = require('sqlite-async');
const fs = require('fs');

const processActivity = (rows) => {
    const result = rows.map(({ timestamp, data }) => {
        const { activity, health } = JSON.parse(data);
        return [timestamp, ...Object.values(activity), ...Object.values(health)].join(',');
    });

    const [firstRow] = rows;
    const { activity, health } = JSON.parse(firstRow.data);

    result.unshift(['timestamp', ...Object.keys(activity), ...Object.keys(health)].join(','));

    return result;
};

const processCollection = (rows, id) => {
    const result = rows.map(({ timestamp, data }) => {
        const c = JSON.parse(data).collections.find((collection) => collection.id === id);
        return [timestamp, ...Object.values(c)].join(',');
    });

    const [firstRow] = rows;
    const { collections } = JSON.parse(firstRow.data);

    result.unshift(['timestamp', ...Object.keys(collections[0])].join(','));

    return result;
};

const init = async () => {
    const db = await Database.open('snapshots.db');
    const rows = await db.all('SELECT data,timestamp FROM master_snapshots WHERE type=1 ORDER BY timestamp');

    const [, , id] = process.argv || [];
    const result = id ? processCollection(rows, id) : processActivity(rows);

    fs.writeFileSync(`${id || 'stats'}.csv`, result.join('\n'));
};

init();
