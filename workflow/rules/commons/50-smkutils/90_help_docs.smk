"""Module containing utility rules
for improving workflow usability.

TODO - add rules to trigger generating
auto docs? see placeholder in
commons::05_docgen.smk
"""

localrules:
    show_help,


rule show_help:
    """Rule to print a readable version
    of all supported config options.
    Dumps the same info into the file
    wd/help.generic-config.txt
    """
    output:
        "show-help",
    retries: 0
    message:
        "Dumping help info to file: help.generic-config.txt"
    run:
        import textwrap as twr
        import io
        import sys

        wrapper = twr.TextWrapper(
            width=80,
            initial_indent=4 * " ",
            subsequent_indent=6 * " ",
            fix_sentence_endings=True,
            break_on_hyphens=False,
        )
        # options can be omitted from help
        # to hide options irrelevant to
        # end users
        omit = None

        formatted_help = []
        for option, opt_spec in dataclasses.asdict(OPTIONS).items():
            if option == "omit":
                omit = set(opt_spec.default)
                continue
            wrapped_help = "\n".join(wrapper.wrap(opt_spec.help))
            opt_str = f" Option: {opt_spec.name}\n Help:\n{wrapped_help}\n\n"
            formatted_help.append(((option, opt_spec.name), opt_str))
        assert omit is not None

        buffer = io.StringIO()
        _ = buffer.write("\n====== WORKFLOW HELP =======")
        _ = buffer.write("\n== Generic config options ==\n\n")
        for (option, name), help_str in formatted_help:
            if option in omit or name in omit:
                continue
            _ = buffer.write(help_str)

        with open("help.generic-config.txt", "w") as dump:
            _ = dump.write(buffer.getvalue())

        sys.stdout.write(buffer.getvalue())
        # END OF RUN BLOCK

