import fs from "fs";
import path from "path";

const VALID_EXTENSIONS = [".js", ".ts", ".py"];
const ALLOWED_ROOTS = ["backend", "src", "scripts"]; // ✅ Added scripts
const IGNORE_DIRS = ["node_modules", "__pycache__", ".git", "venv"];
const OUTPUT_FILE = "annotated_structure.md";

function getTopComment(filePath) {
  const lines = fs.readFileSync(filePath, "utf-8").split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.startsWith("//") || trimmed.startsWith("#")) return trimmed;
    if (trimmed.length > 0) return "(no comment)";
  }
  return "(empty file)";
}

function walk(dir, output = []) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (!IGNORE_DIRS.includes(entry.name)) {
        walk(fullPath, output);
      }
    } else {
      const ext = path.extname(entry.name);
      if (VALID_EXTENSIONS.includes(ext)) {
        const relPath = path.relative(process.cwd(), fullPath);
        const comment = getTopComment(fullPath);
        output.push({ path: relPath, comment });
      }
    }
  }
  return output;
}

function generateStructure() {
  const allFiles = [];

  for (const root of ALLOWED_ROOTS) {
    const fullRoot = path.join(process.cwd(), root);
    if (fs.existsSync(fullRoot)) {
      walk(fullRoot, allFiles);
    }
  }

  const markdown = allFiles
    .map((f) => `### ${f.path}\n> ${f.comment}\n`)
    .join("\n");

  fs.writeFileSync(OUTPUT_FILE, markdown);
  console.log(`✅ Structure written to ${OUTPUT_FILE}`);
}

generateStructure();
