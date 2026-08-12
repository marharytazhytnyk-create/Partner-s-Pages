/* Підписує картку періодом і датою оновлення, читаючи їх із самого звіту.
   Так сторінка партнера не потребує правок після кожного автооновлення.
   Через file:// fetch заблокований — тоді підпис просто не з'являється. */

(function () {
  var card = document.querySelector('.card[href]');
  if (!card) return;

  var slot = card.querySelector('.card-meta');
  if (!slot) return;

  fetch(card.getAttribute('href'))
    .then(function (response) {
      return response.ok ? response.text() : Promise.reject();
    })
    .then(function (html) {
      function grab(label) {
        var match = html.match(label + ':\\s*<strong>([^<]+)</strong>');
        return match ? match[1].trim() : '';
      }

      var parts = [];
      var period = grab('Період');
      var updated = grab('Оновлено');

      if (period) parts.push(period);
      if (updated) parts.push('оновлено ' + updated);

      slot.textContent = parts.join(' \u00b7 ');
    })
    .catch(function () {});
})();
