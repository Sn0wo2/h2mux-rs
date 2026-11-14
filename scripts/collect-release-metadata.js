const { appendFileSync } = require("node:fs")
const { execFileSync } = require("node:child_process")

const { packages } = JSON.parse(execFileSync("cargo", ["metadata", "--no-deps", "--format-version", "1"], { encoding: "utf8" }))
if (
  process.env.VERIFY_VERSION === "true" &&
  packages.some((pkg) =>
    pkg.version !== process.env.VERSION &&
    (pkg.targets.some((target) => target.kind.includes("bin")) || pkg.publish == null || pkg.publish.includes("crates-io")),
  )
) throw new Error(`Expected released packages to have version ${process.env.VERSION}`)
appendFileSync(process.env.GITHUB_OUTPUT, `has_publishable_crates=${packages.some((pkg) => pkg.publish == null || pkg.publish.includes("crates-io"))}\nsha=${execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim()}\n`)
