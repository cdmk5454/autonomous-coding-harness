import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { generateText, type ModelMessage } from "ai";
import Ajv2020 from "ajv/dist/2020.js";
import type { ClientLLM } from "@browserbasehq/stagehand";

const apiKey = process.env.OPENCODE_GO_API_KEY;
const baseURL = process.env.OPENCODE_GO_BASE_URL ?? "https://opencode.ai/zen/go/v1";
const modelId = process.env.OPENCODE_GO_MODEL ?? "deepseek-v4.1-flash";
const userAgent = process.env.OPENCODE_GO_USER_AGENT ?? "kkm-harness-stagehand/0.99";
const sessionId = process.env.OPENCODE_GO_SESSION_ID ?? "stagehand-canary-001";

if (!apiKey) throw new Error("OPENCODE_GO_API_KEY is missing");

const opencodeGo = createOpenAICompatible({
  name: "opencode-go",
  baseURL,
  apiKey,
  headers: {
    "User-Agent": userAgent,
    "x-opencode-session": sessionId,
  },
});

const deepseekModel = opencodeGo.chatModel(modelId);
const ajv = new Ajv2020({ allErrors: true, strict: false });

function asArray<T>(value: T | T[]): T[] { return Array.isArray(value) ? value : [value]; }

function toModelMessages(stagehandMessages: any[]): ModelMessage[] {
  return stagehandMessages.map((message) => {
    const blocks = asArray(message.content);
    if (message.role === "user") {
      const content: any[] = [];
      for (const block of blocks) {
        switch (block.type) {
          case "text": content.push({ type: "text", text: block.text }); break;
          case "image": content.push({ type: "image", image: block.data, mediaType: block.mimeType }); break;
          case "tool_use": throw new Error("DeepSeek ClientLLM: tool_use is disabled in bounded QA mode");
          case "tool_result": throw new Error("DeepSeek ClientLLM: tool_result is disabled in bounded QA mode");
          default: throw new Error(`DeepSeek ClientLLM: unsupported user content block: ${block.type}`);
        }
      }
      return { role: "user", content } as ModelMessage;
    }
    if (message.role === "assistant") {
      const content: any[] = [];
      for (const block of blocks) {
        if (block.type !== "text") throw new Error(`DeepSeek ClientLLM: unsupported assistant content block: ${block.type}`);
        content.push({ type: "text", text: block.text });
      }
      return { role: "assistant", content } as ModelMessage;
    }
    throw new Error(`DeepSeek ClientLLM: unsupported message role: ${message.role}`);
  });
}

function normalizeUsage(usage: any) {
  if (!usage) return undefined;
  const inputTokens = usage.inputTokens ?? usage.promptTokens ?? 0;
  const outputTokens = usage.outputTokens ?? usage.completionTokens ?? 0;
  const totalTokens = usage.totalTokens ?? inputTokens + outputTokens;
  const normalized: any = { inputTokens, outputTokens, totalTokens };
  const reasoningTokens = usage.reasoningTokens ?? usage.outputTokenDetails?.reasoningTokens;
  if (typeof reasoningTokens === "number") normalized.reasoningTokens = reasoningTokens;
  const cachedInputTokens = usage.cachedInputTokens ?? usage.inputTokenDetails?.cacheReadTokens;
  if (typeof cachedInputTokens === "number") normalized.cachedInputTokens = cachedInputTokens;
  return normalized;
}

function normalizeStopReason(reason: unknown): string | undefined {
  if (reason == null) return undefined;
  return typeof reason === "string" ? reason : String(reason);
}

function stripMarkdownFence(value: string): string {
  const trimmed = value.trim();
  const fenced = trimmed.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
  return fenced ? fenced[1].trim() : trimmed;
}

function parseJsonValue(value: string): unknown {
  const cleaned = stripMarkdownFence(value);
  try { return JSON.parse(cleaned); } catch {
    const objectStart = cleaned.indexOf("{");
    const objectEnd = cleaned.lastIndexOf("}");
    if (objectStart >= 0 && objectEnd > objectStart) return JSON.parse(cleaned.slice(objectStart, objectEnd + 1));
    const arrayStart = cleaned.indexOf("[");
    const arrayEnd = cleaned.lastIndexOf("]");
    if (arrayStart >= 0 && arrayEnd > arrayStart) return JSON.parse(cleaned.slice(arrayStart, arrayEnd + 1));
    throw new Error("DeepSeek ClientLLM: model did not return parseable JSON");
  }
}

function buildStructuredSystemPrompt(originalSystemPrompt: string | undefined, schema: unknown, correction?: { previousOutput: string; validationErrors: string; }): string {
  const parts = [
    originalSystemPrompt,
    [
      "You are producing machine-readable JSON for a browser QA runtime.",
      "Return exactly ONE valid JSON value and no prose, markdown, or code fences.",
      "The JSON MUST conform exactly to the JSON Schema below.",
      "Respect every JSON type exactly.",
      "Do not rename fields. Do not omit required fields. Do not add fields unless the schema permits them.",
      "",
      "JSON SCHEMA:",
      JSON.stringify(schema),
    ].join("\n"),
  ];
  if (correction) {
    parts.push([
      "Your previous JSON did not validate.",
      "",
      "VALIDATION ERRORS:", correction.validationErrors,
      "",
      "PREVIOUS OUTPUT:", correction.previousOutput,
      "",
      "Return corrected valid JSON only.",
    ].join("\n"));
  }
  return parts.filter(Boolean).join("\n\n");
}

export const deepseekClient: ClientLLM = {
  generate: async (params) => {
    if (params.tools?.length) throw new Error("DeepSeek ClientLLM: arbitrary tool calling is disabled in bounded QA mode");
    const messages = toModelMessages(params.messages);
    const common = {
      model: deepseekModel,
      messages,
      ...(params.temperature !== undefined ? { temperature: params.temperature } : {}),
      ...(params.stopSequences?.length ? { stopSequences: params.stopSequences } : {}),
    };

    if (params.responseFormat?.type === "json_schema") {
      const schema = params.responseFormat.schema as Record<string, unknown>;
      const validate = ajv.compile(schema);
      let correction: { previousOutput: string; validationErrors: string; } | undefined;
      let finalUsage: any;
      let finalFinishReason: unknown;
      for (let attempt = 1; attempt <= 2; attempt += 1) {
        const result = await generateText({
          ...common,
          system: buildStructuredSystemPrompt(params.systemPrompt, schema, correction),
        });
        finalUsage = result.usage;
        finalFinishReason = result.finishReason;
        let parsed: unknown;
        try { parsed = parseJsonValue(result.text); }
        catch (error) {
          if (attempt === 2) throw error;
          correction = { previousOutput: result.text, validationErrors: error instanceof Error ? error.message : String(error) };
          continue;
        }
        if (validate(parsed)) {
          return {
            role: "assistant",
            content: { type: "text", text: JSON.stringify(parsed) },
            outputFormat: "json_schema",
            structuredContent: parsed as any,
            stopReason: normalizeStopReason(finalFinishReason),
            usage: normalizeUsage(finalUsage),
          };
        }
        const validationErrors = ajv.errorsText(validate.errors, { separator: "\n", dataVar: "structuredContent" });
        if (attempt === 2) throw new Error(["DeepSeek ClientLLM: structured JSON failed schema validation after retry.", validationErrors, "Last model output:", result.text].join("\n"));
        correction = { previousOutput: result.text, validationErrors };
      }
      throw new Error("DeepSeek ClientLLM: structured generation exhausted unexpectedly");
    }

    const result = await generateText({ ...common, ...(params.systemPrompt ? { system: params.systemPrompt } : {}) });
    return {
      role: "assistant",
      content: { type: "text", text: result.text },
      outputFormat: "text",
      stopReason: normalizeStopReason(result.finishReason),
      usage: normalizeUsage(result.usage),
    };
  },
};
