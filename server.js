const express = require('express');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;

// PostgreSQL connection pool
const pool = new Pool({
  host: process.env.DB_HOST || 'sc-fenceline-int-dev3.corp.picarro.com',
  port: process.env.DB_PORT || 5433,
  database: process.env.DB_NAME || 'refinery_restored',
  user: process.env.DB_USER || 'ts_user',
  password: process.env.DB_PASSWORD || 'ts_password',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Middleware
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Streaming API endpoint
app.get('/api/fov', async (req, res) => {
  const { surveySessionId, limit = 100000 } = req.query;

  // Validate required parameter
  if (!surveySessionId) {
    return res.status(400).json({ 
      error: 'surveySessionId is required' 
    });
  }

  // Validate numeric parameters
  const limitNum = parseInt(limit, 10);

  if (isNaN(limitNum) || limitNum < 1) {
    return res.status(400).json({ 
      error: 'limit must be a positive integer' 
    });
  }

  // Use a fixed internal batch size for streaming
  const batchSizeNum = 1000;

  // Set response headers for streaming
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Transfer-Encoding', 'chunked');

  let client;
  try {
    // Get a client from the pool
    client = await pool.connect();

    // Build the base query
    const baseQuery = `
      SELECT
        "surveySessionId",
        "speciesName",
        "deviceId",
        ST_AsGeoJSON(coordinates)::json AS coordinates
      FROM public.layer_fov_copy
      WHERE "surveySessionId" = $1
      ORDER BY ctid
    `;

    let offset = 0;
    let isFirstItem = true;
    let totalRows = 0;

    // Write opening bracket for JSON array
    res.write('[');

    // Function to fetch and stream batches
    const fetchAndStreamBatch = async () => {
      try {
        const remainingLimit = limitNum - totalRows;
        const currentBatchSize = Math.min(batchSizeNum, remainingLimit);
        
        if (currentBatchSize <= 0) {
          // Reached the limit, close the response
          res.write(']');
          res.end();
          client.release();
          return;
        }

        const query = `${baseQuery} LIMIT $2 OFFSET $3`;
        const result = await client.query(query, [surveySessionId, currentBatchSize, offset]);

        if (result.rows.length === 0 || totalRows >= limitNum) {
          // No more rows, close the response
          res.write(']');
          res.end();
          client.release();
          return;
        }

        // Stream the batch
        for (const row of result.rows) {
          if (!isFirstItem) {
            res.write(',');
          }
          res.write(JSON.stringify(row));
          isFirstItem = false;
          totalRows++;
        }

        offset += result.rows.length;

        // Continue fetching next batch if we haven't reached the limit
        if (totalRows < limitNum && result.rows.length === batchSizeNum) {
          // Use setImmediate to avoid blocking the event loop
          setImmediate(() => fetchAndStreamBatch());
        } else {
          // Done streaming
          res.write(']');
          res.end();
          client.release();
        }
      } catch (error) {
        console.error('Error fetching batch:', error);
        if (!res.headersSent) {
          res.status(500).json({ 
            error: 'Error streaming data',
            message: error.message 
          });
        } else {
          res.end();
        }
        client.release();
      }
    };

    // Start fetching and streaming batches
    fetchAndStreamBatch();

  } catch (error) {
    console.error('Error connecting to database:', error);
    
    if (!res.headersSent) {
      res.status(500).json({ 
        error: 'Database connection error',
        message: error.message 
      });
    }
    
    if (client) {
      client.release();
    }
  }
});

// Breadcrumb streaming API endpoint
app.get('/api/breadcrumb', async (req, res) => {
  const { surveySessionId, limit = 100000, batchSize = 100 } = req.query;

  // Validate required parameter
  if (!surveySessionId) {
    return res.status(400).json({ 
      error: 'surveySessionId is required' 
    });
  }

  // Validate numeric parameters
  const limitNum = parseInt(limit, 10);
  const batchSizeNum = parseInt(batchSize, 10);

  if (isNaN(limitNum) || limitNum < 1) {
    return res.status(400).json({ 
      error: 'limit must be a positive integer' 
    });
  }

  if (isNaN(batchSizeNum) || batchSizeNum < 1) {
    return res.status(400).json({ 
      error: 'batchSize must be a positive integer' 
    });
  }

  // Set response headers for streaming
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Transfer-Encoding', 'chunked');

  let client;
  try {
    // Get a client from the pool
    client = await pool.connect();

    // Build the base query
    const baseQuery = `
      SELECT
        "surveySessionId",
        ST_AsGeoJSON(coordinates)::json AS coordinates
      FROM public.layer_breadcrumb
      WHERE "surveySessionId" = $1
      ORDER BY ctid
    `;

    let offset = 0;
    let isFirstItem = true;
    let totalRows = 0;

    // Write opening bracket for JSON array
    res.write('[');

    // Function to fetch and stream batches
    const fetchAndStreamBatch = async () => {
      try {
        const remainingLimit = limitNum - totalRows;
        const currentBatchSize = Math.min(batchSizeNum, remainingLimit);
        
        if (currentBatchSize <= 0) {
          // Reached the limit, close the response
          res.write(']');
          res.end();
          client.release();
          return;
        }

        const query = `${baseQuery} LIMIT $2 OFFSET $3`;
        const result = await client.query(query, [surveySessionId, currentBatchSize, offset]);

        if (result.rows.length === 0 || totalRows >= limitNum) {
          // No more rows, close the response
          res.write(']');
          res.end();
          client.release();
          return;
        }

        // Stream the batch
        for (const row of result.rows) {
          if (!isFirstItem) {
            res.write(',');
          }
          res.write(JSON.stringify(row));
          isFirstItem = false;
          totalRows++;
        }

        offset += result.rows.length;

        // Continue fetching next batch if we haven't reached the limit
        if (totalRows < limitNum && result.rows.length === batchSizeNum) {
          // Use setImmediate to avoid blocking the event loop
          setImmediate(() => fetchAndStreamBatch());
        } else {
          // Done streaming
          res.write(']');
          res.end();
          client.release();
        }
      } catch (error) {
        console.error('Error fetching batch:', error);
        if (!res.headersSent) {
          res.status(500).json({ 
            error: 'Error streaming data',
            message: error.message 
          });
        } else {
          res.end();
        }
        client.release();
      }
    };

    // Start fetching and streaming batches
    fetchAndStreamBatch();

  } catch (error) {
    console.error('Error connecting to database:', error);
    
    if (!res.headersSent) {
      res.status(500).json({ 
        error: 'Database connection error',
        message: error.message 
      });
    }
    
    if (client) {
      client.release();
    }
  }
});

// LISA streaming API endpoint
app.get('/api/lisa', async (req, res) => {
  const { surveySessionId, limit = 100000, batchSize = 100 } = req.query;

  // Validate required parameter
  if (!surveySessionId) {
    return res.status(400).json({ 
      error: 'surveySessionId is required' 
    });
  }

  // Validate numeric parameters
  const limitNum = parseInt(limit, 10);
  const batchSizeNum = parseInt(batchSize, 10);

  if (isNaN(limitNum) || limitNum < 1) {
    return res.status(400).json({ 
      error: 'limit must be a positive integer' 
    });
  }

  if (isNaN(batchSizeNum) || batchSizeNum < 1) {
    return res.status(400).json({ 
      error: 'batchSize must be a positive integer' 
    });
  }

  // Set response headers for streaming
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Transfer-Encoding', 'chunked');

  let client;
  try {
    // Get a client from the pool
    client = await pool.connect();

    // Build the base query
    const baseQuery = `
      SELECT
        "surveySessionId",
        ST_AsGeoJSON(coordinates)::json AS coordinates
      FROM public.layer_peak
      WHERE "surveySessionId" = $1
      ORDER BY ctid
    `;

    let offset = 0;
    let isFirstItem = true;
    let totalRows = 0;

    // Write opening bracket for JSON array
    res.write('[');

    // Function to fetch and stream batches
    const fetchAndStreamBatch = async () => {
      try {
        const remainingLimit = limitNum - totalRows;
        const currentBatchSize = Math.min(batchSizeNum, remainingLimit);
        
        if (currentBatchSize <= 0) {
          // Reached the limit, close the response
          res.write(']');
          res.end();
          client.release();
          return;
        }

        const query = `${baseQuery} LIMIT $2 OFFSET $3`;
        const result = await client.query(query, [surveySessionId, currentBatchSize, offset]);

        if (result.rows.length === 0 || totalRows >= limitNum) {
          // No more rows, close the response
          res.write(']');
          res.end();
          client.release();
          return;
        }

        // Stream the batch
        for (const row of result.rows) {
          if (!isFirstItem) {
            res.write(',');
          }
          res.write(JSON.stringify(row));
          isFirstItem = false;
          totalRows++;
        }

        offset += result.rows.length;

        // Continue fetching next batch if we haven't reached the limit
        if (totalRows < limitNum && result.rows.length === batchSizeNum) {
          // Use setImmediate to avoid blocking the event loop
          setImmediate(() => fetchAndStreamBatch());
        } else {
          // Done streaming
          res.write(']');
          res.end();
          client.release();
        }
      } catch (error) {
        console.error('Error fetching batch:', error);
        if (!res.headersSent) {
          res.status(500).json({ 
            error: 'Error streaming data',
            message: error.message 
          });
        } else {
          res.end();
        }
        client.release();
      }
    };

    // Start fetching and streaming batches
    fetchAndStreamBatch();

  } catch (error) {
    console.error('Error connecting to database:', error);
    
    if (!res.headersSent) {
      res.status(500).json({ 
        error: 'Database connection error',
        message: error.message 
      });
    }
    
    if (client) {
      client.release();
    }
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ 
    error: 'Internal server error',
    message: err.message 
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
  console.log(`Streaming APIs available at:`);
  console.log(`  - http://localhost:${PORT}/api/fov`);
  console.log(`  - http://localhost:${PORT}/api/breadcrumb`);
  console.log(`  - http://localhost:${PORT}/api/lisa`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received: closing HTTP server');
  await pool.end();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT signal received: closing HTTP server');
  await pool.end();
  process.exit(0);
});

