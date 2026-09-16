import "dotenv/config";

import {
  browserbase,
  Stagehand,
} from "@browserbasehq/stagehand";

import {
  deepseekClient,
} from "./opencode-go-deepseek-client.js";

const browserbaseApiKey = process.env.BROWSERBASE_API_KEY;

if (!browserbaseApiKey) {
  throw new Error("BROWSERBASE_API_KEY is missing");
}

console.log("[CANARY] browser: Browserbase");
console.log("[CANARY] model: OpenCode Go / DeepSeek V4.1 Flash");
console.log(
  "[CANARY] session:",
  process.env.OPENCODE_GO_SESSION_ID ?? "stagehand-canary-001",
);

const browser = await browserbase.launch({ apiKey: browserbaseApiKey });
let stagehand: Stagehand | undefined;

try {
  stagehand = await Stagehand.create({
    browser,
    model: deepseekClient,
    logging: { level: "info", format: "pretty" },
  });

  const pages = await browser.context.pages();
  if (pages.length === 0) throw new Error("Browserbase session has no page");
  const page = pages[0];
  await page.goto("https://example.com");
  const title = await page.title();
  if (title !== "Example Domain") throw new Error(`Unexpected page title: ${title}`);

  const observed = await stagehand.observe(
    "Find the main heading containing 'Example Domain'",
  );
  if (!observed.data || observed.data.length === 0) {
    throw new Error("Stagehand observe returned no elements");
  }
  console.log("CANARY_PASS");
} catch (error) {
  console.error("CANARY_FAIL");
  console.error(error);
  process.exitCode = 1;
} finally {
  if (stagehand) {
    try { await stagehand.close(); } catch (error) { console.error(error); }
  }
  try { await browser.close(); } catch (error) { console.error(error); }
}
