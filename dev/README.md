# Dev README
Below are some helpful IntelliJ configurations you can set to match our coding style and standards.

## Checkstyle
This project uses checkstyle to format Java code. If developing locally, please setup checkstyle using the following steps.

1. Add the CheckStyle-IDEA plugin to IntelliJ.
- `Settings > Plugins > Marketplace > CheckStyle-IDEA > INSTALL`.
- Restart your IDE if prompted.

2. Configure IntelliJ to use the `checkstyle.xml` file provided in this directory.
- Go to `Settings > Tools > Checkstyle` (this tool location may differ based on your version of IntelliJ).
- Set the version to 8.29.
- Under the `Configuration File` heading, click the `+` symbol to add our specific configuration file.
- Give our file a useful description, such as `GID Java Checks`, and provide the `connectors/dev/checkstyle.xml` path.
- Click `Next` to add the checkstyle file
- Check `Active` next to it once it has been added
- In the top right, set the Scan Scope to `Only Java sources (including tests)`

3. Now, on the bottom tab bar, there should be a `CheckStyle` tab that lets you run Java style checks against using the `Check Project` button.

## Java Import Order
We use the following import order in our Java files. Please update this in `Settings > Editor > Code Style > Java > Imports > Import Layout`:

```
import java.*
import javax.*
<blank line>
import scala.*
<blank line>
import all other imports
<blank line>
import com.getindata.connectors.*
import com.getindata.connectors.internal.*
```
 