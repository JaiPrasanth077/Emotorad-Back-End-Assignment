const { MongoClient, ServerApiVersion } = require('mongodb');
const express = require('express');
const bodyParser = require('body-parser');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
const port = 3000;

const mongoUrl = 'mongodb+srv://jp:jp@cluster0.lmnos.mongodb.net/?retryWrites=true&w=majority'; // Replace with your MongoDB connection string
const dbName = 'contacts';
const collectionName = 'contacts';

const GEOFENCE_RADIUS_METERS = 20; // Set the geofence radius in meters

app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, '')));

let client;
let db;

async function connectToMongo() {
    client = new MongoClient(mongoUrl, {
        serverApi: {
            version: ServerApiVersion.v1,
            strict: true,
            deprecationErrors: true,
        },
    });

    try {
        await client.connect();
        db = client.db(dbName);

        // Creating indexes for frequently queried fields
        await db.collection(collectionName).createIndex({ latitude: 1, longitude: 1 });
        await db.collection(collectionName).createIndex({ linkedId: 1 });
        await db.collection(collectionName).createIndex({ linkPrecedence: 1 });

        console.log('Connected to MongoDB');
    } catch (err) {
        console.error('Error connecting to MongoDB:', err);
        throw err;
    }
}

// Start the server after the MongoDB connection is established
connectToMongo()
    .then(() => {
        app.listen(port, () => {
            console.log(`Server is running on port ${port}`);
        });
    })
    .catch((err) => {
        console.error('Unable to start the server:', err);
    });

function calculateDistance(lat1, lon1, lat2, lon2) {
    //  formula to calculate distance between two coordinates
    const R = 6371e3; // Earth radius in meters
    const φ1 = lat1 * Math.PI / 180;
    const φ2 = lat2 * Math.PI / 180;
    const Δφ = (lat2 - lat1) * Math.PI / 180;
    const Δλ = (lon2 - lon1) * Math.PI / 180;

    const a = Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
              Math.cos(φ1) * Math.cos(φ2) *
              Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    const distance = R * c;
    console.log("Distance")
    console.log(distance);
    return distance;
}

function findContactByLocation(latitude, longitude) {
    if (!db) {
        console.error('MongoDB connection not established');
        return Promise.reject(new Error('MongoDB connection not established'));
    }

    return db.collection(collectionName).findOne({
        latitude: { $exists: true }, // Ensure latitude field exists
        longitude: { $exists: true }, // Ensure longitude field exists
        linkPrecedence: 'primary',
    }).then(primaryContact => {
        if (!primaryContact) {
            return null; // No primary contact found
        }

        const distance = calculateDistance(primaryContact.latitude, primaryContact.longitude, latitude, longitude);

        return distance <= GEOFENCE_RADIUS_METERS ? primaryContact : null;
    });
}

function createContact(email, phoneNumber, latitude, longitude, linkedId = null, linkPrecedence = 'primary') {
    const contact = {
        _id: uuidv4(),
        email,
        phoneNumber,
        latitude,
        longitude,
        linkedId,
        linkPrecedence,
        createdAt: new Date(),
        updatedAt: new Date(),
    };

    db.collection(collectionName).insertOne(contact);
    return contact;
}

function updateContact(contact, email, phoneNumber, latitude, longitude, linkPrecedence = 'secondary') {
    // Always create a new entry for secondary contacts
    const newSecondaryContact = createContact(email, phoneNumber, latitude, longitude, contact._id, 'secondary');

    return newSecondaryContact;
}

app.post('/identify', async (req, res) => {
    const { email, phoneNumber, latitude, longitude } = req.body;

    const existingContact = await findContactByLocation(latitude, longitude);

    if (!existingContact) {
        // Create a new "primary" contact
        const newContact = createContact(email, phoneNumber, latitude, longitude);
        console.log('New Primary Contact Created:', newContact);
        res.status(200).json({
            primaryContactId: newContact._id,
            emails: [newContact.email],
            phoneNumbers: [newContact.phoneNumber],
            secondaryContactIds: [],
        });
    } else {
        // Always create a new entry for secondary contacts
        const newSecondaryContact = updateContact(existingContact, email, phoneNumber, latitude, longitude);
        console.log('New Secondary Contact Created:', newSecondaryContact);

        // Fetch all secondary contacts associated with the primary contact
        const secondaryContacts = await db.collection(collectionName)
            .find({ linkedId: existingContact._id, linkPrecedence: 'secondary' })
            .toArray();

        const secondaryEmails = secondaryContacts.map(contact => contact.email);
        const secondaryPhoneNumbers = secondaryContacts.map(contact => contact.phoneNumber);
        const secondaryContactIds = secondaryContacts.map(contact => contact._id);

        res.status(200).json({
            primaryContactId: existingContact._id,
            emails: [existingContact.email, ...secondaryEmails],
            phoneNumbers: [existingContact.phoneNumber, ...secondaryPhoneNumbers],
            secondaryContactIds: [newSecondaryContact._id, ...secondaryContactIds],
        });
    }
});

process.on('SIGINT', async () => {
    if (client) {
        await client.close();
        console.log('MongoDB connection closed');
    }
    process.exit();
});
