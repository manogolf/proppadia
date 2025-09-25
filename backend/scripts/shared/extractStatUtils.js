// File: backend/scripts/shared/extractStatUtils.js

import { derivePropValue } from "../resolution/derivePropValue.js";

/**
 * Given a prop type and raw stats object, resolve the stat value for that prop.
 * This wraps the derivePropValue function for safe backend use.
 *
 * @param {string} propType
 * @param {object} stats
 * @returns {number|null}
 */
export function extractStatForPropType(propType, stats) {
  return derivePropValue(propType, stats);
}
