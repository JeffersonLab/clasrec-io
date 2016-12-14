# clasrec-io

Standard I/O services for CLAS12 reconstruction using CLARA.

## Services

-   `org.jlab.clas.std.services.convertors.EvioToEvioReader`:

    Reads EVIO events from a file.
    Returns a new event on each request or an error if there was some problem.

-   `org.jlab.clas.std.services.convertors.EvioToEvioWriter`:

    Writes EVIO events to a file.
    Saves the received event to disk or report an error if there was some
    problem.

-   `org.jlab.clas.std.services.convertors.HipoToHipoReader`:

    Reads HIPO events from a file.
    Returns a new event on each request or an error if there was some problem.

-   `org.jlab.clas.std.services.convertors.HipoToHipoWriter`:

    Writes HIPO events to a file.
    Saves the received event to disk or report an error if there was some
    problem.

-   `org.jlab.clas.std.services.system.DataManager`:

    Stages reconstruction files into the local file-system.
    Copies the input file from a shared disk into the local file-system,
    and then moves the reconstructed output file back to the shared disk.

