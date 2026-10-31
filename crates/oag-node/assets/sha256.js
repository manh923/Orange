// SHA-256, computed in this browser. The same value `sha256sum` prints.
//
// Used to timestamp a file with a commitment output (SPEC §7.7) and to check
// one later. **The file never leaves the machine**: it is read here in
// pieces, and only the 32-byte digest is ever shown or sent.
//
// Written out instead of using `crypto.subtle.digest`, which exists only on
// HTTPS and localhost (an explorer opened over a LAN would lose it) and wants
// the whole file in memory at once.
"use strict";

const OAG_SHA256 = (() => {
  const K = new Uint32Array([
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
  ]);
  // How much of a file is read at a time.
  const PIECE = 4 * 1024 * 1024;

  class Sha256 {
    constructor() {
      this.h = new Uint32Array([
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
      ]);
      this.w = new Uint32Array(64);
      this.block = new Uint8Array(64);
      this.filled = 0;
      // Bytes so far. A Number is exact up to 2^53, far past any file.
      this.length = 0;
    }

    // One 64-byte block at `at` in `bytes`.
    compress(bytes, at) {
      const w = this.w;
      for (let i = 0; i < 16; i++) {
        const j = at + i * 4;
        w[i] = (bytes[j] << 24) | (bytes[j + 1] << 16) | (bytes[j + 2] << 8) | bytes[j + 3];
      }
      for (let i = 16; i < 64; i++) {
        const a = w[i - 15];
        const b = w[i - 2];
        const s0 = ((a >>> 7) | (a << 25)) ^ ((a >>> 18) | (a << 14)) ^ (a >>> 3);
        const s1 = ((b >>> 17) | (b << 15)) ^ ((b >>> 19) | (b << 13)) ^ (b >>> 10);
        w[i] = (w[i - 16] + s0 + w[i - 7] + s1) | 0;
      }
      const h = this.h;
      let [a, b, c, d, e, f, g, k] = h;
      for (let i = 0; i < 64; i++) {
        const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
        const ch = (e & f) ^ (~e & g);
        const t1 = (k + S1 + ch + K[i] + w[i]) | 0;
        const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
        const maj = (a & b) ^ (a & c) ^ (b & c);
        const t2 = (S0 + maj) | 0;
        k = g;
        g = f;
        f = e;
        e = (d + t1) | 0;
        d = c;
        c = b;
        b = a;
        a = (t1 + t2) | 0;
      }
      h[0] += a;
      h[1] += b;
      h[2] += c;
      h[3] += d;
      h[4] += e;
      h[5] += f;
      h[6] += g;
      h[7] += k;
    }

    update(bytes) {
      let at = 0;
      this.length += bytes.length;
      if (this.filled > 0) {
        const take = Math.min(64 - this.filled, bytes.length);
        this.block.set(bytes.subarray(0, take), this.filled);
        this.filled += take;
        at = take;
        if (this.filled < 64) return;
        this.compress(this.block, 0);
        this.filled = 0;
      }
      for (; at + 64 <= bytes.length; at += 64) this.compress(bytes, at);
      this.block.set(bytes.subarray(at), 0);
      this.filled = bytes.length - at;
    }

    // The digest as 32 bytes. The object is spent afterwards.
    digest() {
      const bits = this.length * 8;
      const tail = new Uint8Array(this.filled < 56 ? 64 - this.filled : 128 - this.filled);
      tail[0] = 0x80;
      const view = new DataView(tail.buffer);
      view.setUint32(tail.length - 8, Math.floor(bits / 2 ** 32));
      view.setUint32(tail.length - 4, bits >>> 0);
      const length = this.length;
      this.update(tail);
      this.length = length;
      const out = new Uint8Array(32);
      const outView = new DataView(out.buffer);
      for (let i = 0; i < 8; i++) outView.setUint32(i * 4, this.h[i]);
      return out;
    }
  }

  function toHex(bytes) {
    return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  }

  // The SHA-256 of a File (or Blob), as lowercase hex. `progress(done, total)`
  // is called after each piece.
  async function ofFile(file, progress) {
    const sha = new Sha256();
    for (let at = 0; at < file.size; at += PIECE) {
      const piece = await file.slice(at, at + PIECE).arrayBuffer();
      sha.update(new Uint8Array(piece));
      if (progress) progress(Math.min(at + PIECE, file.size), file.size);
    }
    return toHex(sha.digest());
  }

  return { Sha256, ofFile, toHex };
})();
