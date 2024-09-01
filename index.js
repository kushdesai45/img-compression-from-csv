const express = require('express');
const multer = require('multer');
const csvParser = require('csv-parser');
const Jimp = require('jimp');
const fs = require('fs-extra');
const path = require('path');
const axios = require('axios');
const mysql = require('mysql2/promise');
const { v4: uuidv4 } = require('uuid');

const app = express();
const upload = multer({ dest: 'uploads/' });

// MySQL connection configuration
const dbConfig = {
    host: 'localhost',
    user: 'your_username',   // Replace with your MySQL username
    password: 'your_password',   // Replace with your MySQL password
    database: 'your_database',   // Replace with your MySQL database name
};

// Establish MySQL connection
async function connectToDatabase() {
    try {
        const connection = await mysql.createConnection(dbConfig);
        console.log('Connected to MySQL database');
        return connection;
    } catch (error) {
        console.error('Error connecting to MySQL:', error);
        throw error;
    }
}

// POST endpoint to upload CSV and process data
app.post('/upload-csv', upload.single('file'), async (req, res) => {
    let connection;
    try {
        if (!req.file) {
            return res.status(400).json({ message: 'No file uploaded or file field name mismatch' });
        }

        // Connect to MySQL
        connection = await connectToDatabase();

        const filePath = req.file.path;
        const results = [];

        fs.createReadStream(filePath)
            .pipe(csvParser())
            .on('data', (row) => {
                results.push(row);
            })
            .on('end', async () => {
                // Generate a unique request ID
                const requestId = uuidv4();

                const updatedRows = await processCSVData(results);

                // Save updated data to MySQL with the unique request ID
                await saveToDatabase(connection, updatedRows, requestId);

                // Return the unique request ID in the response
                res.json({ message: 'Data processed and stored in MySQL', requestId });

                fs.removeSync(filePath);
            })
            .on('error', (err) => {
                console.error(err);
                res.status(500).send('Error processing the CSV file');
            });
    } catch (error) {
        console.error(error);
        res.status(500).send('Error processing the CSV file');
    } finally {
        if (connection) {
            await connection.end();
        }
    }
});

// GET endpoint to retrieve data by request ID
app.get('/data/:requestId', async (req, res) => {
    let connection;
    try {
        const { requestId } = req.params;

        // Connect to MySQL
        connection = await connectToDatabase();

        const [rows] = await connection.execute(
            'SELECT serial_number, product_name, input_image_urls, compressed_image_urls FROM product_images WHERE request_id = ?',
            [requestId]
        );

        if (rows.length === 0) {
            return res.status(404).json({ message: 'No data found for the provided request ID' });
        }

        res.json({ requestId, data: rows });
    } catch (error) {
        console.error('Error retrieving data:', error);
        res.status(500).json({ message: 'Error retrieving data' });
    } finally {
        if (connection) {
            await connection.end();
        }
    }
});

function getSafeFilename(url) {
    const cleanUrl = url.split('?')[0];
    return path.basename(cleanUrl);
}

async function processCSVData(rows) {
    const updatedRows = [];

    for (const row of rows) {
        const { 'Serial Number': serialNumber, 'Product Name': productName, 'Input Image Urls': inputImageUrls } = row;

        if (!serialNumber || !productName || !inputImageUrls) {
            throw new Error('Invalid CSV data');
        }

        const imageUrls = inputImageUrls.split(',').map(url => url.trim());
        const compressedImageUrls = [];

        for (const imageUrl of imageUrls) {
            const compressedUrl = await compressImage(imageUrl);
            compressedImageUrls.push(compressedUrl);
        }

        const newRow = {
            serialNumber,
            productName,
            inputImageUrls,
            compressedImageUrls: compressedImageUrls.join(', ')
        };

        updatedRows.push(newRow);
    }

    return updatedRows;
}

async function compressImage(imageUrl) {
    const imageFileName = getSafeFilename(imageUrl);
    const outputDir = path.join('compressed-images');
    const outputFilePath = path.join(outputDir, `compressed-${imageFileName}`);

    await fs.ensureDir(outputDir);

    try {
        const { data: imageBuffer } = await axios({
            url: imageUrl,
            responseType: 'arraybuffer',
        });

        const image = await Jimp.read(imageBuffer);
        await image
            .resize(image.bitmap.width * 0.5, Jimp.AUTO)
            .quality(60)
            .writeAsync(outputFilePath);

        return path.join(outputDir, `compressed-${imageFileName}`);
    } catch (err) {
        console.error(`Failed to process image: ${imageUrl}`, err);
        throw err;
    }
}

async function saveToDatabase(connection, rows, requestId) {
    const insertQuery = `
        INSERT INTO product_images (serial_number, product_name, input_image_urls, compressed_image_urls, request_id)
        VALUES (?, ?, ?, ?, ?)
    `;

    for (const row of rows) {
        const { serialNumber, productName, inputImageUrls, compressedImageUrls } = row;

        await connection.execute(insertQuery, [serialNumber, productName, inputImageUrls, compressedImageUrls, requestId]);
    }
}

app.listen(3000, () => {
    console.log('Server started on http://localhost:3000');
});
