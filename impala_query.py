import subprocess
import re


class ImpalaError(Exception):
    pass


def run_query_impala(query: str, db: str = "aa_work", output=None):
    command = [
        "impala-shell",
        "-i",
        "s-m-prddhdata02.internal.sky.de",
        "-k",
        "--ssl",
        "--ca_cert=/opt/cloudera/security/x509/truststore.pem",
        "-B",
        f"-d {db}",
        f'-q {query}'
    ]

    if output is not None:
        command.append(f"-o{output}")

    print("Running query:")
    print(query)

    out = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    if out.returncode != 0:
        raise ImpalaError(
            "\n\nimpala-shell had a non-zero exit status:\n\n" +
            out.stdout.decode("utf-8")
        )
    process_output = out.stdout.decode("utf-8").split("\n")
    process_output = [l for l in process_output
                        if re.match("^Query submitted.*|^Fetched.*", l) is not None]
    process_output = "\n".join(process_output)
    print("\n")
    print(process_output)