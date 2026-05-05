(function () {
  const body = document.body;
  const links = document.querySelectorAll("[data-section-link]");
  const sections = Array.from(links)
    .map((link) => document.querySelector(link.getAttribute("href")))
    .filter(Boolean);

  const setActive = () => {
    let active = sections[0] ? sections[0].id : "";
    for (const section of sections) {
      const rect = section.getBoundingClientRect();
      if (rect.top < 170) active = section.id;
    }
    links.forEach((link) => {
      link.classList.toggle("active", link.getAttribute("href") === "#" + active);
    });
  };

  document.querySelectorAll("[data-toggle-expert]").forEach((button) => {
    button.addEventListener("click", () => {
      body.classList.toggle("show-expert");
      const appendix = document.querySelector("#expert-appendix");
      if (body.classList.contains("show-expert") && appendix) {
        appendix.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
  });

  window.addEventListener("scroll", setActive, { passive: true });
  setActive();
})();
