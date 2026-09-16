/**
 * 0.99.2.1 QA Backend Hotfix - Stagehand v4 QA runner (fallback provider).
 *
 * Input JSON (qa_browser_providers.py):
 *   { qa_execution_id, session_id, scenario: { scenario_id, title, start_url,
 *     acceptance, steps } }
 *
 * Executes deterministic read-only-capable steps on Browserbase with the
 * OpenCode Go / DeepSeek V4.1 Flash ClientLLM, then writes a structured
 * result JSON. The runner is an evidence provider only; it never decides
 * canonical Job status.
 *
 * Safety: only goto/click/type/assert_contains/assert steps are executed and
 * only when the harness-side side-effect policy already admitted the
 * scenario. The runner refuses forbidden action tokens defensively.
 */
import "dotenv/config";

import { readFileSync, writeFileSync } from "node:fs";
import { argv, exit } from "node:process";

import { browserbase, Stagehand } from "@browserbasehq/stagehand";
import { deepseekClient } from "./opencode-go-deepseek-client.js";

interface QaStep { action?: string; value?: string; text?: string; }
interface QaInput {
  qa_execution_id?: string;
  session_id?: string;
  scenario?: {
    scenario_id?: string;
    title?: string;
    start_url?: string;
    acceptance?: string[];
    steps?: QaStep[];
  };
}

const FORBIDDEN_STEP_TOKENS = [
  "save", "delete", "send", "approval", "approve", "payment", "submit",
  "drop", "insert into", "update ",
];

function argValue(flag: string): string | undefined {
  const index = argv.indexOf(flag);
  return index >= 0 ? argv[index + 1] : undefined;
}

function isForbidden(action: string, value: string): boolean {
  const text = `${action} ${value}`.toLowerCase();
  return FORBIDDEN_STEP_TOKENS.some((token) => text.includes(token));
}

interface AssertionRecord { name: string; passed: boolean; detail: string; }

async function main(): Promise<void> {
  const inputPath = argValue("--input");
  const outputPath = argValue("--output");
  if (!inputPath || !outputPath) {
    console.error("qa-runner: --input and --output are required");
    exit(2);
    return;
  }

  let input: QaInput;
  try {
    input = JSON.parse(readFileSync(inputPath, "utf-8")) as QaInput;
  } catch (error) {
    console.error("qa-runner: input json unreadable", error);
    exit(2);
    return;
  }

  const scenario = input.scenario ?? {};
  const scenarioId = scenario.scenario_id ?? "unknown-scenario";
  const sessionId = input.session_id ?? `harness-qa-${input.qa_execution_id ?? "adhoc"}`;
  const assertions: AssertionRecord[] = [];
  let failureCode = "";
  let failureMessage = "";
  let browserVersion = "";

  const browser = await browserbase.launch({ apiKey: process.env.BROWSERBASE_API_KEY });
  let stagehand: Stagehand | undefined;

  try {
    stagehand = await Stagehand.create({
      browser,
      model: deepseekClient,
      logging: { level: "warn", format: "pretty" },
    });
    const pages = await browser.context.pages();
    const target = pages.length > 0 ? pages[0] : await browser.context.newPage();
    browserVersion = await target.evaluate(() => navigator.userAgent);
    const startUrl = scenario.start_url ?? "";
    if (startUrl) await target.goto(startUrl, { timeout: 45_000 });
    const pageText = async (): Promise<string> =>
      (await target.evaluate(() => document.body.innerText)).replace(/\s+/g, " ");

    for (const step of scenario.steps ?? []) {
      const action = String(step.action ?? "").toLowerCase();
      const value = String(step.value ?? "");
      if (!action) continue;
      if (isForbidden(action, String(step.text ?? "")) || isForbidden(action, value)) {
        failureCode = "QA_SCENARIO_FORBIDDEN";
        failureMessage = `forbidden action token in step: ${action}`;
        break;
      }
      if (action === "goto" && value) {
        await target.goto(value, { timeout: 45_000 });
      } else if (action === "assert_contains" && value) {
        const expected = value.replace(/\s+/g, " ");
        const content = await pageText();
        const passed = content.includes(expected);
        assertions.push({ name: `page contains: ${value}`, passed, detail: passed ? "text found" : "text not found on page" });
        if (!passed) { failureCode = "QA_ASSERTION_FAILED"; failureMessage = `page does not contain '${value}'`; break; }
      } else if (action === "assert" && value) {
        const observed = await stagehand.observe(value);
        const passed = Boolean(observed.data && observed.data.length > 0);
        assertions.push({ name: `observe: ${value}`, passed, detail: passed ? `${observed.data.length} element(s) observed` : "no matching element observed" });
        if (!passed) { failureCode = "QA_EXPECTED_ELEMENT_MISSING"; failureMessage = `expected element missing: ${value}`; break; }
      } else if (action === "click" && value) {
        await stagehand.act(`click on ${value}`);
        assertions.push({ name: `click: ${value}`, passed: true, detail: "clicked" });
      } else if (action === "type" && value) {
        await stagehand.act(`type into ${value}: ${String(step.text ?? "")}`);
        assertions.push({ name: `type: ${value}`, passed: true, detail: "typed" });
      } else {
        failureCode = "QA_PROVIDER_COMPATIBILITY_ERROR";
        failureMessage = `unsupported step action: ${action}`;
        break;
      }
    }

    if (!failureCode && (scenario.steps ?? []).length === 0) {
      const content = await pageText();
      for (const item of scenario.acceptance ?? []) {
        const expected = String(item).replace(/\s+/g, " ");
        const passed = content.includes(expected);
        assertions.push({ name: `acceptance: ${item}`, passed, detail: passed ? "acceptance text found" : "acceptance text not found" });
        if (!passed) { failureCode = "QA_ASSERTION_FAILED"; failureMessage = `acceptance not satisfied: ${item}`; break; }
      }
    }
  } catch (error) {
    if (!failureCode) {
      failureCode = "QA_BROWSER_INFRA_FAILURE";
      failureMessage = error instanceof Error ? error.message : String(error);
    }
  } finally {
    try { await stagehand?.close(); } catch { /* close failures are not evidence */ }
    try { await browser.close(); } catch { /* close failures are not evidence */ }
  }

  const passed = assertions.length > 0 && !failureCode;
  const result = {
    schema_revision: "qa-browser-runner/1",
    provider: "STAGEHAND",
    status: passed ? "PASS" : "FAIL",
    passed,
    recovery_used: false,
    scenario_id: scenarioId,
    qa_execution_id: input.qa_execution_id ?? "",
    failure_code: failureCode,
    failure_message: failureMessage.slice(0, 1000),
    assertions: assertions.slice(0, 40),
    browser: "Browserbase",
    browser_version: browserVersion ?? "",
    usage: {},
    provider_metadata: { session_id: sessionId },
  };
  writeFileSync(outputPath, JSON.stringify(result, null, 2), "utf-8");
  console.log(`qa-runner: ${scenarioId} -> ${result.status}`);
  exit(passed ? 0 : 1);
}

main().catch((error) => {
  console.error("qa-runner: fatal", error);
  exit(3);
});
