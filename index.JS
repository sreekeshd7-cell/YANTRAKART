import express from 'express';
import cors from 'cors';
import { initializeApp } from 'firebase/app';
import {
  getFirestore,
  collection,
  addDoc,
  serverTimestamp
} from 'firebase/firestore';

const app = express();
const port = 5000;

// Firebase config (replace with your own config)
const firebaseConfig = {
  apiKey: "AIzaSyCyjqvnrgCHt3SR4_kD3glRt55ImeXOxxA",
  authDomain: "yantrakartcu.firebaseapp.com",
  projectId: "yantrakartcu",
  storageBucket: "yantrakartcu.firebasestorage.app",
  messagingSenderId: "958180945232",
  appId: "1:958180945232:web:18e049c7682da22b239055",
  measurementId: "G-87FE485DTH"
};
    

const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);

// Middleware
app.use(cors());
app.use(express.json());

// POST endpoint to submit feedback
app.post('/submit-feedback', async (req, res) => {
  const { name, phone, feedback } = req.body;

  if (!name || !phone || !feedback) {
    return res.status(400).json({ error: 'All fields are required.' });
  }

  try {
    // 1. Add feedback to 'feedbacks' collection
    const feedbackRef = await addDoc(collection(db, 'feedbacks'), {
      name,
      phone,
      feedback,
      approved: false,
      timestamp: serverTimestamp()
    });

    // 2. Add notification for admin to 'notifications' collection
    await addDoc(collection(db, 'notifications'), {
      userEmail: 'sreekeshd7@gmail.com', // Use your admin identifier or email here
      type: 'feedback',
      message: `📝 New feedback submitted by <b>${name}</b> (${phone}):<br>${feedback}`,
      feedbackId: feedbackRef.id,
      createdAt: serverTimestamp()
    });

    res.status(200).json({ message: 'Feedback submitted successfully.', id: feedbackRef.id });
  } catch (error) {
    console.error('Error submitting feedback:', error);
    res.status(500).json({ error: 'Failed to submit feedback.' });
  }
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
