// 📄 File: scripts/utils/generateDateBuckets.js

import { format, parseISO, addDays, differenceInDays } from "date-fns";

function generateDateBuckets(start, end, numBuckets) {
  const totalDays = differenceInDays(end, start) + 1;
  const daysPerBucket = Math.ceil(totalDays / numBuckets);

  const buckets = [];
  for (let i = 0; i < numBuckets; i++) {
    const bucketStart = addDays(start, i * daysPerBucket);
    const bucketEnd = addDays(
      start,
      Math.min((i + 1) * daysPerBucket - 1, totalDays - 1)
    );
    buckets.push({
      start: format(bucketStart, "yyyy-MM-dd"),
      end: format(bucketEnd, "yyyy-MM-dd"),
    });
  }

  return buckets;
}

// CLI Usage
// Example: node scripts/utils/generateDateBuckets.js 2023-03-30 2024-10-01 4
if (import.meta.url === `file://${process.argv[1]}`) {
  const [startStr, endStr, numBucketsStr] = process.argv.slice(2);
  const start = parseISO(startStr);
  const end = parseISO(endStr);
  const numBuckets = parseInt(numBucketsStr, 10);

  if (!start || !end || isNaN(numBuckets)) {
    console.error(
      "Usage: node generateDateBuckets.js <start> <end> <numBuckets>"
    );
    process.exit(1);
  }

  const result = generateDateBuckets(start, end, numBuckets);
  console.log("🪣 Generated Buckets:");
  result.forEach((b, i) => {
    console.log(`Bucket ${i + 1}: --start=${b.start} --end=${b.end}`);
  });
}

export { generateDateBuckets };
