"""План знижок NOA жовтень–грудень 2026 — документ для партнера (.docx)."""

from pathlib import Path

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
from docx.shared import Cm, Pt, RGBColor

OUTPUT = Path(__file__).with_name("Plan_Znizhky_Zhovten_Gruden_2026.docx")

ACCENT = RGBColor(0x22, 0x8B, 0x54)
MUTED = RGBColor(0x6B, 0x70, 0x76)
DARK = RGBColor(0x1A, 0x1A, 0x1A)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
HEADER_BG = "228B54"
ALT_BG = "F4F7F5"


def set_run_font(run, *, size=11, bold=False, color=DARK, italic=False):
    run.font.name = "Calibri"
    run.font.size = Pt(size)
    run.font.bold = bold
    run.font.italic = italic
    run.font.color.rgb = color
    run._element.rPr.rFonts.set(qn("w:eastAsia"), "Calibri")


def set_cell_shading(cell, hex_color):
    tc = cell._tc
    tc_pr = tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:fill"), hex_color)
    shd.set(qn("w:val"), "clear")
    tc_pr.append(shd)


def set_cell_margins(cell, **kwargs):
    tc = cell._tc
    tc_pr = tc.get_or_add_tcPr()
    tc_mar = OxmlElement("w:tcMar")
    for key, value in kwargs.items():
        node = OxmlElement(f"w:{key}")
        node.set(qn("w:w"), str(value))
        node.set(qn("w:type"), "dxa")
        tc_mar.append(node)
    tc_pr.append(tc_mar)


def write_cell(cell, text, *, size=10, bold=False, color=DARK, fill=None, center=False):
    cell.text = ""
    p = cell.paragraphs[0]
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER if center else WD_ALIGN_PARAGRAPH.LEFT
    p.paragraph_format.space_before = Pt(2)
    p.paragraph_format.space_after = Pt(2)
    run = p.add_run(text)
    set_run_font(run, size=size, bold=bold, color=color)
    set_cell_margins(cell, top=60, bottom=60, left=80, right=80)
    if fill:
        set_cell_shading(cell, fill)


def set_base_font(document):
    style = document.styles["Normal"]
    style.font.name = "Calibri"
    style.font.size = Pt(11)
    style.element.rPr.rFonts.set(qn("w:eastAsia"), "Calibri")
    style.paragraph_format.space_after = Pt(8)
    style.paragraph_format.line_spacing = 1.15


def title_block(document):
    heading = document.add_paragraph()
    heading.paragraph_format.space_after = Pt(4)
    run = heading.add_run("NOA · план знижок жовтень–грудень 2026")
    set_run_font(run, size=22, bold=True, color=ACCENT)

    subtitle = document.add_paragraph()
    subtitle.paragraph_format.space_after = Pt(12)
    run = subtitle.add_run(
        "Знижка повністю за рахунок Bolt · одна пропозиція на бренд"
    )
    set_run_font(run, size=11, color=MUTED, italic=True)


def h1(document, text):
    p = document.add_paragraph()
    p.paragraph_format.space_before = Pt(16)
    p.paragraph_format.space_after = Pt(6)
    run = p.add_run(text)
    set_run_font(run, size=14, bold=True, color=ACCENT)
    return p


def para(document, text):
    p = document.add_paragraph()
    p.paragraph_format.space_after = Pt(8)
    run = p.add_run(text)
    set_run_font(run, size=11)
    return p


def add_table(document, headers, rows, widths):
    table = document.add_table(rows=1, cols=len(headers))
    table.alignment = WD_TABLE_ALIGNMENT.LEFT
    table.autofit = False

    for i, text in enumerate(headers):
        write_cell(
            table.rows[0].cells[i],
            text,
            size=10,
            bold=True,
            color=WHITE,
            fill=HEADER_BG,
        )

    for r_idx, row in enumerate(rows):
        cells = table.add_row().cells
        fill = ALT_BG if r_idx % 2 == 0 else "FFFFFF"
        for i, text in enumerate(row):
            write_cell(cells[i], text, size=10, fill=fill)

    for row in table.rows:
        for cell, width in zip(row.cells, widths):
            cell.width = Cm(width)

    tbl = table._tbl
    tbl_pr = tbl.tblPr if tbl.tblPr is not None else OxmlElement("w:tblPr")
    borders = OxmlElement("w:tblBorders")
    for edge in ("top", "left", "bottom", "right", "insideH", "insideV"):
        el = OxmlElement(f"w:{edge}")
        el.set(qn("w:val"), "single")
        el.set(qn("w:sz"), "4")
        el.set(qn("w:space"), "0")
        el.set(qn("w:color"), "D5DDD8")
        borders.append(el)
    tbl_pr.append(borders)
    return table


def build():
    document = Document()
    for section in document.sections:
        section.page_width = Cm(21.0)
        section.page_height = Cm(29.7)
        section.left_margin = Cm(1.8)
        section.right_margin = Cm(1.8)
        section.top_margin = Cm(1.6)
        section.bottom_margin = Cm(1.6)

    set_base_font(document)
    title_block(document)

    h1(document, "Коли і кому показуємо акцію")
    para(
        document,
        "У жовтні стартуємо з 12-го і тримаємо акцію до кінця місяця. "
        "Листопад і грудень — повний місяць. Кожного періоду інша група гостей, "
        "щоб акція набирала та повертала аудиторію по черзі.",
    )
    add_table(
        document,
        ["Період", "Дати", "Кому", "Навіщо"],
        [
            [
                "Жовтень",
                "12–31 жовтня",
                "Гості, які вже замовляли",
                "Коротше вікно — одразу повертаємо своїх, поки кухня ще в пам’яті.",
            ],
            [
                "Листопад",
                "1–30 листопада",
                "Нові гості — Poke, Una, Emily, Guest Emily, ТАШ",
                "Перше замовлення — розширити базу перед святами.",
            ],
            [
                "Листопад",
                "1–30 листопада",
                "Усі гості — Tomatina і NOA Asia Special",
                "−20% на 2–3 топ-страви: акцію бачить кожен, хто відкриє заклад.",
            ],
            [
                "Грудень",
                "1–31 грудня",
                "Знову ті, хто вже замовляв",
                "Святкові сети й замовлення на компанію охочіше беруть свої гості.",
            ],
        ],
        widths=[3.0, 3.4, 5.4, 5.6],
    )

    h1(document, "Що запускаємо на брендах")
    para(
        document,
        "Для більшості брендів тип знижки й мінімальний чек не змінюються "
        "з 12 жовтня до кінця грудня. Змінюється лише те, кому акція світиться "
        "(див. таблицю вище). Виняток — листопад на Tomatina і NOA Asia Special.",
    )
    add_table(
        document,
        ["Бренд", "Жовтень і грудень", "Мінімальний чек"],
        [
            ["Tomatina", "Доставка 0 ₴, до 80 ₴", "500 ₴"],
            ["Poke LULU", "Доставка 0 ₴, до 80 ₴", "500 ₴"],
            ["NOA Asia Special", "Доставка 0 ₴, до 80 ₴", "700 ₴"],
            ["Una Pinsa", "Меню −20%, до 200 ₴", "650 ₴"],
            ["Emily Brooklyn Pizza", "Меню −20%, до 200 ₴", "700 ₴"],
            ["NOA Guest Emily", "Меню −20%, до 200 ₴", "700 ₴"],
            ["ТАШ", "Меню −20%, до 200 ₴", "700 ₴"],
        ],
        widths=[5.4, 8.0, 4.0],
    )
    para(
        document,
        "Poke, Una, Emily, Guest Emily і ТАШ у листопаді лишаються з тією ж "
        "знижкою, що в таблиці — лише для нових гостей закладу.",
    )

    h1(document, "Листопад · Tomatina і NOA Asia Special")
    para(
        document,
        "На листопад на цих двох брендах замість безкоштовної доставки ставимо "
        "−20% на 2–3 найпопулярніші страви. Для всіх гостей, повністю за рахунок Bolt. "
        "Позиції узгоджуємо перед запуском. Окремий мінімальний чек не ставимо: "
        "знижка вже на конкретних стравах. Безкоштовну доставку в листопаді "
        "на цих брендах не підключаємо — одночасно одна пропозиція.",
    )
    add_table(
        document,
        ["Бренд", "Листопад 1–30", "Для кого", "Мінчек"],
        [
            [
                "Tomatina",
                "−20% на 2–3 топ-позиції меню",
                "Усі гості",
                "немає",
            ],
            [
                "NOA Asia Special",
                "−20% на 2–3 топ-позиції меню",
                "Усі гості",
                "немає",
            ],
        ],
        widths=[4.2, 5.6, 3.6, 4.0],
    )

    h1(document, "Зверніть увагу")
    para(
        document,
        "Так як знижка підключається за рахунок Bolt — замовлення вам "
        "надходитимуть за повною ціною.",
    )

    document.save(OUTPUT)
    print(f"Saved {OUTPUT}")


if __name__ == "__main__":
    build()
