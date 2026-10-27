/** Only a display choice is saved in this browser. */
export const DISPLAY_KEY = 'data-horizon-display-v1';
export function displayPreferences(value) {
    return { scheme: ['dark', 'light', 'system'].includes(value?.scheme) ? value.scheme : 'dark' };
}
export function resolvedScheme(preference, systemDark) {
    return preference === 'system' ? (systemDark ? 'dark' : 'light') : preference === 'light' ? 'light' : 'dark';
}
export function contrastRatio(first, second) {
    function luminance(hex) {
        const values = hex.replace('#', '').match(/../g).map(value => parseInt(value, 16) / 255)
            .map(value => value <= .04045 ? value / 12.92 : ((value + .055) / 1.055) ** 2.4);
        return .2126 * values[0] + .7152 * values[1] + .0722 * values[2];
    }
    const a = luminance(first), b = luminance(second);
    return (Math.max(a, b) + .05) / (Math.min(a, b) + .05);
}
