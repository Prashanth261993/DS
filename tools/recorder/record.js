// One-off recorder: captures a webm of the live dashboard for the showcase site.
// Usage: node record.js
const { chromium } = require("playwright");

(async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1100, height: 620 },
    recordVideo: { dir: "out", size: { width: 1100, height: 620 } },
  });
  const page = await context.newPage();
  await page.goto("http://localhost:5173/", { waitUntil: "networkidle" });
  await page.waitForTimeout(14000); // record ~14s of live updates
  await context.close();            // finalizes the video file
  await browser.close();
  console.log("done");
})();
