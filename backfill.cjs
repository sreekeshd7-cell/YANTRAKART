const admin = require('firebase-admin');
const functions = require('firebase-functions'); // Added for Cloud Functions
admin.initializeApp();

const db = admin.firestore();
const auth = admin.auth();

// Improved backfill function with pagination
async function backfillUsers(nextPageToken) {
  try {
    const listUsersResult = await auth.listUsers(1000, nextPageToken);
    
    // Batch write for efficiency
    const batch = db.batch();
    let batchCount = 0;

    for (const userRecord of listUsersResult.users) {
      const user = userRecord.toJSON();
      const userRef = db.collection('users').doc(user.uid);

      batch.set(userRef, {
        displayName: user.displayName || "",
        email: user.email || "",
        phone: user.phoneNumber || "",
        referralCode: generateReferralCode(),
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      // Commit every 500 operations (Firestore batch limit)
      if (++batchCount % 500 === 0) {
        await batch.commit();
        batch = db.batch();
      }
    }

    // Commit remaining operations
    if (batchCount % 500 !== 0) await batch.commit();

    // Pagination
    if (listUsersResult.pageToken) {
      await backfillUsers(listUsersResult.pageToken);
    }
  } catch (error) {
    console.error('Error backfilling users:', error);
    throw error; // Important for Cloud Functions error reporting
  }
}

function generateReferralCode(length = 8) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  return Array.from({ length }, () => 
    chars.charAt(Math.floor(Math.random() * chars.length))
  ).join('');
}

// Scheduled Cloud Function
exports.scheduledBackfill = functions.pubsub
  .schedule('every 1 hours')
  .timeZone('Asia/Kolkata') // Update with your timezone
  .onRun(async (context) => {
    console.log('Starting user backfill...');
    await backfillUsers();
    console.log('Backfill completed successfully');
    return null;
  });
