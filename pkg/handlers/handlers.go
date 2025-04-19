package handlers

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/JayJamieson/csv-api/pkg/db"
	"github.com/JayJamieson/csv-api/pkg/models"
	"github.com/labstack/echo/v4"
)

type Handler struct {
	DB *db.DB
}

func NewHandler(db *db.DB) *Handler {
	return &Handler{
		DB: db,
	}
}

func (h *Handler) LoadCSV(c echo.Context) error {
	ctx := c.Request().Context()

	csvURL := c.QueryParam("url")
	name := c.QueryParam("name")

	var reader io.Reader
	var filename string

	if csvURL != "" {

		resp, err := http.Get(csvURL)
		if err != nil {
			return createErrorResponse(c, http.StatusBadRequest, "URL fetch error", err.Error())
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return createErrorResponse(c, http.StatusBadRequest, "URL fetch error",
				fmt.Sprintf("Status code: %d", resp.StatusCode))
		}

		reader = resp.Body

		parsedURL, err := url.Parse(csvURL)
		if err != nil {
			return createErrorResponse(c, http.StatusBadRequest, "Invalid URL", err.Error())
		}

		path := parsedURL.Path
		for i := len(path) - 1; i >= 0; i-- {
			if path[i] == '/' {
				filename = path[i+1:]
				break
			}
		}

		if filename == "" {
			filename = "downloaded.csv"
		}
	} else if name != "" {

		if name == "" {
			return createErrorResponse(c, http.StatusBadRequest, "Missing parameter",
				"Filename must be provided via 'name' parameter when uploading CSV content")
		}

		reader = c.Request().Body
		filename = name
	} else {
		return createErrorResponse(c, http.StatusBadRequest, "Missing parameter",
			"Either 'url' or 'name' parameter must be provided")
	}

	csvTable, err := h.DB.ImportCSVFromReader(ctx, filename, reader)
	if err != nil {
		return createErrorResponse(c, http.StatusInternalServerError, "CSV import error", err.Error())
	}

	endpoint := fmt.Sprintf("%s://%s/api/memory/%s", c.Scheme(), c.Request().Host, csvTable.ID)

	return c.JSON(http.StatusOK, models.LoadResponse{
		OK:       true,
		Endpoint: endpoint,
	})
}

func (h *Handler) QueryMemoryCSV(c echo.Context) error {
	ctx := c.Request().Context()

	id := c.Param("uuid")
	if id == "" {
		return createErrorResponse(c, http.StatusBadRequest, "Missing parameter", "UUID is required")
	}

	csvTable, err := h.DB.GetCSVTable(ctx, id)
	if err != nil {
		return createErrorResponse(c, http.StatusNotFound, "Resource not found", err.Error())
	}

	size, _ := strconv.Atoi(c.QueryParam("_size"))
	offset, _ := strconv.Atoi(c.QueryParam("_offset"))
	sortCol := c.QueryParam("_sort")
	if sortCol == "" {
		sortCol = c.QueryParam("_sort_desc")
	}
	sortDesc := c.QueryParam("_sort_desc") != ""
	shape := c.QueryParam("_shape")
	if shape == "" {
		shape = "objects"
	}
	showRowID := c.QueryParam("_rowid") != "hide"
	showTotal := c.QueryParam("_total") != "hide"

	columns, objectRows, arrayRows, total, queryTime, err := h.DB.QueryDuckDBTable(
		ctx, id, csvTable.TableName, size, offset, sortCol, sortDesc, showRowID,
	)
	if err != nil {
		return createErrorResponse(c, http.StatusInternalServerError, "Query error", err.Error())
	}

	baseResp := models.DataResponseBase{
		OK:      true,
		QueryMS: queryTime,
		Columns: columns,
	}

	if showTotal {
		baseResp.Total = total
	}

	if shape == "objects" {
		resp := models.DataResponseObjects{
			DataResponseBase: baseResp,
			Rows:             objectRows,
		}
		return c.JSON(http.StatusOK, resp)
	}

	resp := models.DataResponseArray{
		DataResponseBase: baseResp,
		Rows:             arrayRows,
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) QueryCSV(c echo.Context) error {
	ctx := c.Request().Context()

	id := c.Param("uuid")
	if id == "" {
		return createErrorResponse(c, http.StatusBadRequest, "Missing parameter", "UUID is required")
	}

	csvTable, err := h.DB.GetCSVTable(ctx, id)
	if err != nil {
		return createErrorResponse(c, http.StatusNotFound, "Resource not found", err.Error())
	}

	if !csvTable.Persisted {

		return c.Redirect(http.StatusTemporaryRedirect,
			fmt.Sprintf("/api/memory/%s%s", id, c.QueryString()))
	}

	size, _ := strconv.Atoi(c.QueryParam("_size"))
	offset, _ := strconv.Atoi(c.QueryParam("_offset"))
	sortCol := c.QueryParam("_sort")
	if sortCol == "" {
		sortCol = c.QueryParam("_sort_desc")
	}
	sortDesc := c.QueryParam("_sort_desc") != ""
	shape := c.QueryParam("_shape")
	if shape == "" {
		shape = "objects"
	}
	showRowID := c.QueryParam("_rowid") != "hide" // Default to show
	showTotal := c.QueryParam("_total") != "hide" // Default to show

	columns, objectRows, arrayRows, total, queryTime, err := h.DB.QueryCSVTable(
		ctx, csvTable.TableName, size, offset, sortCol, sortDesc, showRowID,
	)
	if err != nil {
		return createErrorResponse(c, http.StatusInternalServerError, "Query error", err.Error())
	}

	baseResp := models.DataResponseBase{
		OK:      true,
		QueryMS: queryTime,
		Columns: columns,
	}

	if showTotal {
		baseResp.Total = total
	}

	if shape == "objects" {
		resp := models.DataResponseObjects{
			DataResponseBase: baseResp,
			Rows:             objectRows,
		}
		return c.JSON(http.StatusOK, resp)
	}

	resp := models.DataResponseArray{
		DataResponseBase: baseResp,
		Rows:             arrayRows,
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) PersistCSV(c echo.Context) error {
	ctx := c.Request().Context()

	id := c.Param("uuid")
	if id == "" {
		return createErrorResponse(c, http.StatusBadRequest, "Missing parameter", "UUID is required")
	}

	csvTable, err := h.DB.GetCSVTable(ctx, id)
	if err != nil {
		return createErrorResponse(c, http.StatusNotFound, "Resource not found", err.Error())
	}

	if csvTable.Persisted {
		return c.JSON(http.StatusOK, models.PersistResponse{
			OK:        true,
			Message:   "Table already persisted to Turso",
			Persisted: true,
		})
	}

	if err := h.DB.PersistToTurso(ctx, id); err != nil {
		return createErrorResponse(c, http.StatusInternalServerError, "Persistence error", err.Error())
	}

	endpoint := fmt.Sprintf("%s://%s/api/%s", c.Scheme(), c.Request().Host, id)

	return c.JSON(http.StatusOK, models.PersistResponse{
		OK:        true,
		Message:   fmt.Sprintf("Successfully persisted to Turso. You can now query at %s", endpoint),
		Persisted: true,
	})
}

func createErrorResponse(c echo.Context, status int, error string, message string) error {
	resp := models.ErrorResponse{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Error:     error,
		Message:   message,
	}
	return c.JSON(status, resp)
}
