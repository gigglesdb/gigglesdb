'use strict';
document.querySelectorAll('pre').forEach(function (pre) {
  var button = document.createElement('button');
  button.type = 'button';
  button.className = 'copy-button';
  button.textContent = 'Copy';
  button.setAttribute('aria-label', 'Copy code');
  button.addEventListener('click', async function () {
    var code = pre.querySelector('code');
    if (!code) return;
    try {
      await navigator.clipboard.writeText(code.textContent);
      button.textContent = 'Copied';
    } catch (error) {
      button.textContent = 'Select code';
      var selection = window.getSelection();
      var range = document.createRange();
      range.selectNodeContents(code);
      selection.removeAllRanges();
      selection.addRange(range);
    }
    window.setTimeout(function () { button.textContent = 'Copy'; }, 1800);
  });
  pre.appendChild(button);
});
var toggle = document.querySelector('.nav-toggle');
if (toggle) {
  var nav = document.getElementById(toggle.getAttribute('aria-controls'));
  if (nav) {
    toggle.addEventListener('click', function () {
      var open = toggle.getAttribute('aria-expanded') !== 'true';
      toggle.setAttribute('aria-expanded', String(open));
      nav.classList.toggle('is-open', open);
    });
  }
}
