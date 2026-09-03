const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

app.use(express.json());
app.use(express.static(path.join(__dirname)));

// Direct file serving
app.get('/driver', (req, res) => {
  res.sendFile(path.join(__dirname, 'driver.html'));
});

app.get('/customer', (req, res) => {
  res.sendFile(path.join(__dirname, 'customer.html'));
});

// Database setup
const db = new sqlite3.Database(path.join(__dirname, 'velox.db'), (err) => {
  if (err) console.error('Database connection error:', err);
  else console.log('Connected to SQLite database.');
});

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS rides (
      ride_id TEXT PRIMARY KEY,
      driver_id TEXT,
      customer_id TEXT,
      vehicle_type TEXT,
      pickup TEXT,
      drop_loc TEXT,
      fare REAL,
      driver_payout REAL,
      platform_cut REAL,
      status TEXT,
      otp TEXT,
      rating INTEGER,
      tip REAL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS drivers (
      driver_id TEXT PRIMARY KEY,
      name TEXT,
      vehicle_plate TEXT,
      total_trips INTEGER DEFAULT 0,
      total_earnings REAL DEFAULT 0,
      platform_cut REAL DEFAULT 0
    )
  `);

  db.run(`
    INSERT OR IGNORE INTO drivers (driver_id, name, vehicle_plate, total_trips, total_earnings, platform_cut)
    VALUES ('drv_101', 'Ramesh Rao', 'TS 03 EQ 4812', 0, 0, 0)
  `);
});

// State containers
const onlineDrivers = new Map();
const activeRides = new Map();

// REST APIs
app.get('/api/driver/:driverId/metrics', (req, res) => {
  const { driverId } = req.params;
  db.get('SELECT * FROM drivers WHERE driver_id = ?', [driverId], (err, row) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json(row || { total_trips: 0, total_earnings: 0, platform_cut: 0 });
  });
});

app.get('/api/rides/history', (req, res) => {
  db.all('SELECT * FROM rides ORDER BY created_at DESC LIMIT 10', [], (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json(rows);
  });
});

// Real-time Gateway
io.on('connection', (socket) => {
  // Driver goes online
  socket.on('driver:online', (driverData) => {
    onlineDrivers.set(driverData.driverId, {
      ...driverData,
      socketId: socket.id,
      status: 'AVAILABLE'
    });
  });

  // Customer requests ride
  socket.on('ride:request', (data) => {
    const rideId = data.rideId;
    const grossFare = Number(data.fare) || 126;
    const platformCut = Math.round(grossFare * 0.15);
    const driverPayout = grossFare - platformCut;
    const fixedOtp = '1234';

    const routeCoordinates = [
      data.pickup,
      [17.9850, 79.5300],
      [17.9940, 79.5420],
      [18.0050, 79.5510],
      data.drop
    ];

    const rideRecord = {
      rideId,
      customerSocketId: socket.id,
      pickup: data.pickup,
      drop: data.drop,
      vehicleType: data.vehicleType,
      fare: grossFare,
      driverPayout,
      platformCut,
      otp: fixedOtp,
      path: routeCoordinates,
      selectedRouteName: data.selectedRouteName,
      distanceKm: data.distanceKm,
      status: 'SEARCHING'
    };

    activeRides.set(rideId, rideRecord);

    db.run(
      `INSERT INTO rides (ride_id, customer_id, vehicle_type, pickup, drop_loc, fare, driver_payout, platform_cut, status, otp)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        rideId,
        socket.id,
        data.vehicleType,
        JSON.stringify(data.pickup),
        JSON.stringify(data.drop),
        grossFare,
        driverPayout,
        platformCut,
        'SEARCHING',
        fixedOtp
      ]
    );

    // Broadcast to available online drivers
    for (const [drvId, drv] of onlineDrivers.entries()) {
      if (drv.status === 'AVAILABLE') {
        io.to(drv.socketId).emit('driver:incoming_alert', {
          rideId,
          fare: grossFare,
          driverPayout,
          vehicleType: data.vehicleType,
          selectedRouteName: data.selectedRouteName,
          distanceKm: data.distanceKm,
          path: routeCoordinates
        });
      }
    }
  });

  // Driver accepts ride
  socket.on('driver:accept', ({ rideId, driverId }) => {
    const ride = activeRides.get(rideId);
    const driver = onlineDrivers.get(driverId);

    if (ride && ride.status === 'SEARCHING') {
      ride.status = 'ASSIGNED';
      ride.driverId = driverId;
      ride.driverSocketId = socket.id;

      if (driver) driver.status = 'ON_TRIP';

      db.run('UPDATE rides SET status = ?, driver_id = ? WHERE ride_id = ?', ['ASSIGNED', driverId, rideId]);

      // Confirm to accepting driver
      socket.emit('driver:accepted_success', {
        rideId,
        customerSocketId: ride.customerSocketId
      });

      // Notify customer
      io.to(ride.customerSocketId).emit('ride:assigned', {
        driverSocketId: socket.id,
        name: driver ? driver.name : 'Ramesh Rao',
        vehiclePlate: driver ? driver.vehiclePlate : 'TS 03 EQ 4812'
      });

      // Revoke from other drivers
      socket.broadcast.emit('driver:alert_revoked', { rideId });
    } else {
      socket.emit('driver:accept_failed', { message: 'Ride already accepted or canceled.' });
    }
  });

  // Driver declines ride
  socket.on('driver:decline', ({ rideId }) => {
    // Left available for future redispatch queue logic
  });

  // OTP Verification
  socket.on('driver:verify_otp', ({ rideId, inputOtp }) => {
    const ride = activeRides.get(rideId);
    if (ride && ride.otp === inputOtp) {
      ride.status = 'IN_PROGRESS';
      db.run('UPDATE rides SET status = ? WHERE ride_id = ?', ['IN_PROGRESS', rideId]);

      socket.emit('otp:result', { success: true });
      io.to(ride.customerSocketId).emit('ride:started');
    } else {
      socket.emit('otp:result', { success: false });
    }
  });

  // Telemetry stream
  socket.on('ride:telemetry', (data) => {
    const ride = activeRides.get(data.rideId);
    if (ride) {
      io.to(ride.customerSocketId).emit('ride:telemetry_update', data);
    }
  });

  // Trip Completion
  socket.on('driver:complete', ({ rideId }) => {
    const ride = activeRides.get(rideId);
    if (ride) {
      ride.status = 'COMPLETED';
      db.run('UPDATE rides SET status = ? WHERE ride_id = ?', ['COMPLETED', rideId]);

      db.run(
        `UPDATE drivers 
         SET total_trips = total_trips + 1, 
             total_earnings = total_earnings + ?, 
             platform_cut = platform_cut + ? 
         WHERE driver_id = ?`,
        [ride.driverPayout, ride.platformCut, ride.driverId]
      );

      const driver = onlineDrivers.get(ride.driverId);
      if (driver) driver.status = 'AVAILABLE';

      io.to(ride.customerSocketId).emit('ride:completed_client');
      socket.emit('driver:refresh_metrics');
    }
  });

  // Rating & Tip
  socket.on('customer:rate_and_tip', ({ rideId, rating, tip }) => {
    const numericTip = Number(tip) || 0;
    db.run('UPDATE rides SET rating = ?, tip = ? WHERE ride_id = ?', [rating, numericTip, rideId]);

    const ride = activeRides.get(rideId);
    if (ride && numericTip > 0) {
      db.run('UPDATE drivers SET total_earnings = total_earnings + ? WHERE driver_id = ?', [numericTip, ride.driverId]);
      if (ride.driverSocketId) {
        io.to(ride.driverSocketId).emit('driver:refresh_metrics');
      }
    }
  });

  // Two-way messaging
  socket.on('msg:to_driver', ({ rideId, message }) => {
    io.emit('msg:received_driver', { rideId, message });
  });

  socket.on('msg:to_customer', ({ rideId, message }) => {
    io.emit('msg:received_customer', { rideId, message });
  });

  // WebRTC VoIP Signaling
  socket.on('call:initiate', ({ targetSocketId, offer }) => {
    io.to(targetSocketId).emit('call:incoming', { fromSocketId: socket.id, offer });
  });

  socket.on('call:accept', ({ targetSocketId, answer }) => {
    io.to(targetSocketId).emit('call:accepted', { answer });
  });

  socket.on('call:ice_candidate', ({ targetSocketId, candidate }) => {
    io.to(targetSocketId).emit('call:ice_candidate', { candidate });
  });

  socket.on('call:end', ({ targetSocketId }) => {
    io.to(targetSocketId).emit('call:ended');
  });

  socket.on('disconnect', () => {
    for (const [id, driver] of onlineDrivers.entries()) {
      if (driver.socketId === socket.id) {
        onlineDrivers.delete(id);
        break;
      }
    }
  });
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`Velox backend running on port ${PORT}`);
});