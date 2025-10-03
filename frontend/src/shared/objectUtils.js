// src/shared/objectUtils.js
export function setIfMissing(target, key, newValue, currentValue) {
  if (
    currentValue === null ||
    currentValue === undefined ||
    (typeof currentValue === "number" && isNaN(currentValue))
  ) {
    target[key] = newValue;
  }
}
