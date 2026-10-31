// The explorer's one piece of JavaScript: on a transaction page with a
// commitment output, check whether a file is the one recorded.
//
// The file is hashed in this browser (sha256.js) and compared with the
// commitments on the page. **Nothing is sent anywhere**, and the page works
// the same without this script; it only adds the check.
"use strict";

document.addEventListener("DOMContentLoaded", () => {
  const input = document.getElementById("ts-file");
  const out = document.getElementById("ts-out");
  if (!input || !out) return;
  const recorded = (input.dataset.commitments || "").split(" ").filter(Boolean);
  input.disabled = false;

  input.onchange = async () => {
    const file = input.files[0];
    if (!file) return;
    out.className = "";
    out.textContent = "hashing…";
    try {
      const digest = await OAG_SHA256.ofFile(file, (done, total) => {
        out.textContent = `hashing… ${total ? Math.floor((done * 100) / total) : 100}%`;
      });
      if (recorded.includes(digest)) {
        out.className = "ok";
        out.textContent = `match — the SHA-256 of ${file.name} is ${digest}, the commitment recorded in this transaction.`;
      } else {
        out.className = "warn";
        out.textContent = `no match — the SHA-256 of ${file.name} is ${digest}, which is not recorded here.`;
      }
    } catch (e) {
      out.className = "warn";
      out.textContent = `cannot read the file: ${e.message}`;
    }
  };
});
