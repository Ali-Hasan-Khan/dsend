(function () {
  "use strict";

  var navToggle = document.getElementById("navToggle");
  var navLinks = document.getElementById("navLinks");

  if (navToggle && navLinks) {
    navToggle.addEventListener("click", function () {
      navLinks.classList.toggle("open");
    });

    navLinks.addEventListener("click", function (e) {
      if (e.target.closest("a")) {
        navLinks.classList.remove("open");
      }
    });
  }

  document.querySelectorAll(".code-copy").forEach(function (btn) {
    btn.addEventListener("click", function () {
      var block = btn.closest(".code-block");
      if (!block) return;
      var pre = block.querySelector("pre");
      if (!pre) return;

      var text = pre.innerText.replace(/\n$/, "");

      var done = function () {
        var original = btn.textContent;
        btn.textContent = "Copied";
        btn.classList.add("copied");
        setTimeout(function () {
          btn.textContent = original;
          btn.classList.remove("copied");
        }, 1600);
      };

      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(done, function () {
          fallbackCopy(text);
          done();
        });
      } else {
        fallbackCopy(text);
        done();
      }
    });
  });

  function fallbackCopy(text) {
    var ta = document.createElement("textarea");
    ta.value = text;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand("copy");
    } catch (e) {}
    document.body.removeChild(ta);
  }

  var sidebarLinks = Array.prototype.slice.call(
    document.querySelectorAll(".docs-sidebar nav a[href^='#']")
  );

  if (sidebarLinks.length) {
    var sections = sidebarLinks
      .map(function (link) {
        var el = document.querySelector(link.getAttribute("href"));
        return el ? { link: link, el: el } : null;
      })
      .filter(Boolean);

    var setActive = function (id) {
      sidebarLinks.forEach(function (link) {
        link.classList.toggle("active", link.getAttribute("href") === "#" + id);
      });
    };

    var spy = function () {
      var pos = window.scrollY + 96;
      var current = null;
      sections.forEach(function (s) {
        if (s.el.offsetTop <= pos) current = s.el.id;
      });
      if (current) setActive(current);
    };

    if ("IntersectionObserver" in window) {
      var observer = new IntersectionObserver(
        function (entries) {
          entries.forEach(function (entry) {
            if (entry.isIntersecting) {
              setActive(entry.target.id);
            }
          });
        },
        { rootMargin: "-20% 0px -65% 0px" }
      );
      sections.forEach(function (s) {
        observer.observe(s.el);
      });
    } else {
      window.addEventListener("scroll", spy);
      spy();
    }
  }
})();
