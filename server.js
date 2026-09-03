const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const sqlite3 = require('sqlite3').verbose();

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*', methods: ['GET', 'POST'] } });

const PORT = process.env.PORT || 4000;

// SQLite database setup
const db = new sqlite3.Database('./velox.db');
db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS rides (
      id TEXT PRIMARY KEY,
      pickup_address TEXT,
      drop_address TEXT,
      vehicle_type TEXT,
      route_name TEXT,
      distance_km REAL,
      fare_grand_total REAL,
      driver_payout REAL,
      driver_name TEXT,
      rating INTEGER DEFAULT 5,
      tip_amount REAL DEFAULT 0,
      status TEXT,
      completed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
});

const onlineDrivers = new Map();
const activeRides = new Map();
const dispatchTimers = new Map();

// Driver Metrics API
app.get('/api/driver/:id/metrics', (req, res) => {
  db.all(
    `SELECT COUNT(*) as total_trips, 
            COALESCE(SUM(driver_payout + tip_amount), 0) as total_earnings,
            COALESCE(SUM(fare_grand_total - driver_payout), 0) as platform_cut
     FROM rides WHERE status = 'COMPLETED'`,
    [],
    (err, rows) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json(rows[0] || { total_trips: 0, total_earnings: 0, platform_cut: 0 });
    }
  );
});

// Trip History API
app.get('/api/rides/history', (req, res) => {
  db.all('SELECT * FROM rides ORDER BY completed_at DESC', [], (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json(rows);
  });
});

io.on('connection', (socket) => {
  // Driver goes online
  socket.on('driver:online', (driverData) => {
    onlineDrivers.set(driverData.driverId, { 
      ...driverData, 
      socketId: socket.id, 
      status: 'AVAILABLE' 
    });
  });

  // Relay note/message from passenger to driver
  socket.on('msg:to_driver', ({ rideId, message }) => {
    io.emit('msg:received_driver', { rideId, message });
  });

  // Relay note/message from driver to customer
  socket.on('msg:to_customer', ({ rideId, message }) => {
    io.emit('msg:received_customer', { rideId, message });
  });
});

  socket.on('ride:request', (payload) => {
    const rideId = `ride_${Date.now()}`;
    const otp = "1234"; // Fixed PIN for testing

    const rideData = {
      ...payload,
      rideId,
      startOtp: otp,
      status: 'SEARCHING',
      customerSocketId: socket.id
    };

    activeRides.set(rideId, rideData);
    socket.emit('ride:created', { rideId, otp });

    onlineDrivers.forEach((driver) => {
      if (driver.status === 'AVAILABLE') {
        io.to(driver.socketId).emit('driver:incoming_alert', {
          rideId,
          pickupAddress: payload.pickupAddress,
          dropAddress: payload.dropAddress,
          vehicleType: payload.vehicleType,
          selectedRouteName: payload.selectedRouteName,
          distanceKm: payload.distanceKm,
          driverPayout: payload.driverPayout,
          path: payload.path,
          timeoutSeconds: 15
        });
      }
    });

    const timer = setTimeout(() => {
      const ride = activeRides.get(rideId);
      if (ride && ride.status === 'SEARCHING') {
        io.to(ride.customerSocketId).emit('ride:timeout', {
          message: 'No driver accepted within the window. Tap to search again.'
        });
        onlineDrivers.forEach((drv) => {
          io.to(drv.socketId).emit('driver:alert_revoked', { rideId });
        });
        activeRides.delete(rideId);
      }
      dispatchTimers.delete(rideId);
    }, 15000);

    dispatchTimers.set(rideId, timer);
  });

  socket.on('driver:accept', ({ rideId, driverId }) => {
    const ride = activeRides.get(rideId);
    if (!ride || ride.status !== 'SEARCHING') {
      socket.emit('driver:accept_failed', { message: 'Ride expired or already assigned.' });
      return;
    }

    if (dispatchTimers.has(rideId)) {
      clearTimeout(dispatchTimers.get(rideId));
      dispatchTimers.delete(rideId);
    }

    ride.status = 'ASSIGNED';
    ride.driverId = driverId;
    ride.driverSocketId = socket.id;

    const driver = onlineDrivers.get(driverId);
    if (driver) driver.status = 'BUSY';

    onlineDrivers.forEach((drv) => {
      if (drv.driverId !== driverId) {
        io.to(drv.socketId).emit('driver:alert_revoked', { rideId });
      }
    });

    io.to(ride.customerSocketId).emit('ride:matched', {
      rideId,
      driverName: driver ? driver.name : 'Ramesh Rao',
      vehiclePlate: driver ? driver.vehiclePlate : 'TS 03 EQ 4812',
      driverSocketId: socket.id
    });

    socket.emit('driver:accepted_success', {
      rideId,
      customerSocketId: ride.customerSocketId,
      path: ride.path,
      selectedRouteName: ride.selectedRouteName
    });
  });

  socket.on('driver:decline', ({ rideId }) => {
    socket.emit('driver:alert_revoked', { rideId });
  });

  socket.on('driver:verify_otp', ({ rideId, inputOtp }) => {
    const ride = activeRides.get(rideId);
    if (!ride) return;

    if (ride.startOtp === inputOtp.trim()) {
      ride.status = 'STARTED';
      socket.emit('otp:result', { success: true });
      io.to(ride.customerSocketId).emit('ride:started');
    } else {
      socket.emit('otp:result', { success: false });
    }
  });

  socket.on('ride:telemetry', ({ rideId, lat, lng, progress }) => {
    const ride = activeRides.get(rideId);
    if (ride) {
      io.to(ride.customerSocketId).emit('ride:track_step', { lat, lng, progress });
    }
  });

  socket.on('driver:complete', ({ rideId }) => {
    const ride = activeRides.get(rideId);
    if (!ride) return;

    const driver = onlineDrivers.get(ride.driverId);
    if (driver) driver.status = 'AVAILABLE';
    const driverName = driver ? driver.name : 'Ramesh Rao';

    db.run(
      `INSERT INTO rides (id, pickup_address, drop_address, vehicle_type, route_name, distance_km, fare_grand_total, driver_payout, driver_name, status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        ride.rideId,
        ride.pickupAddress,
        ride.dropAddress,
        ride.vehicleType || 'Bike',
        ride.selectedRouteName,
        ride.distanceKm,
        ride.grandTotal,
        ride.driverPayout,
        driverName,
        'COMPLETED'
      ]
    );

    io.to(ride.customerSocketId).emit('ride:completed', { rideId: ride.rideId });
    socket.emit('driver:completed_success');
  });

  // Rider submits review & optional tip
  socket.on('ride:submit_feedback', ({ rideId, rating, tip }) => {
    const tipVal = Number(tip) || 0;
    const ratingVal = Number(rating) || 5;

    db.run(
      `UPDATE rides SET rating = ?, tip_amount = ? WHERE id = ?`,
      [ratingVal, tipVal, rideId],
      (err) => {
        if (!err) {
          activeRides.delete(rideId);
          // Broadcast metric refresh to drivers
          io.emit('driver:refresh_metrics');
        }
      }
    );
  });

  // WebRTC Signaling
  socket.on('call:initiate', ({ targetSocketId, offer }) => {
    io.to(targetSocketId).emit('call:incoming', { fromSocketId: socket.id, offer });
  });
  socket.on('call:accept', ({ targetSocketId, answer }) => {
    io.to(targetSocketId).emit('call:accepted', { fromSocketId: socket.id, answer });
  });
  socket.on('call:ice_candidate', ({ targetSocketId, candidate }) => {
    io.to(targetSocketId).emit('call:ice_candidate', { candidate });
  });
  socket.on('call:end', ({ targetSocketId }) => {
    io.to(targetSocketId).emit('call:ended');
  });

  socket.on('disconnect', () => {
    for (const [driverId, driver] of onlineDrivers.entries()) {
      if (driver.socketId === socket.id) {
        onlineDrivers.delete(driverId);
        break;
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`Velox backend running on port ${PORT}`);
});