(function () {
  function copyText(text, button) {
    function done() {
      var original = button.textContent;
      button.textContent = "copied";
      window.setTimeout(function () {
        button.textContent = original;
      }, 1200);
    }

    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done).catch(function () {
        fallbackCopy(text, done);
      });
    } else {
      fallbackCopy(text, done);
    }
  }

  function fallbackCopy(text, done) {
    var area = document.createElement("textarea");
    area.value = text;
    area.setAttribute("readonly", "");
    area.style.position = "absolute";
    area.style.left = "-9999px";
    document.body.appendChild(area);
    area.select();
    try {
      document.execCommand("copy");
      done();
    } catch (_err) {
      // Ignore: copy is progressive enhancement.
    }
    document.body.removeChild(area);
  }

  document.querySelectorAll(".code-block, .command-block").forEach(function (block) {
    var pre = block.querySelector("pre");
    var label = block.querySelector(".label");
    if (!pre || !label || label.querySelector(".copy-btn")) {
      return;
    }
    var button = document.createElement("button");
    button.type = "button";
    button.className = "copy-btn";
    button.textContent = "copy";
    button.addEventListener("click", function () {
      copyText(pre.textContent || "", button);
    });
    label.appendChild(button);
  });
})();
